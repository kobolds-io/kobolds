const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);

const uuid = @import("uuid");

const constants = @import("../constants.zig");
const Message = @import("../message.zig").Message;
const Accept = @import("../message.zig").Accept;
const Ping = @import("../message.zig").Ping;
const Pong = @import("../message.zig").Pong;
const Request = @import("../message.zig").Request;
const Reply = @import("../message.zig").Reply;

const Worker = @import("./worker.zig").Worker;

const MessagePool = @import("../message_pool.zig").MessagePool;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;

const Connection = @import("../connection.zig").Connection;
const ProtocolError = @import("../errors.zig").ProtocolError;
const IO = @import("../io.zig").IO;

/// static configuration used to configure the node
pub const NodeConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",
    /// tcp connections are accepted on this port
    port: u16 = 4000,

    /// number of worker threads, default is 1 worker per cpu core
    worker_thread_count: usize = 1,
};

/// A node is the central building block for communicating between nodes
/// on either the same or different hosts.
pub const Node = struct {
    const Self = @This();
    /// id of the node
    id: uuid.Uuid,

    allocator: std.mem.Allocator,

    running: bool,

    /// configuration file used by the node
    config: NodeConfig,

    /// socket used to accept new connections to the node
    listener_socket: posix.socket_t,

    /// shared reference to IO
    io: *IO,

    /// Reference to the global message pool
    message_pool: *MessagePool,

    /// Trackers to accept new connections
    accept_submitted: bool,
    accept_completion: *IO.Completion,

    /// Queue of message pointers that still need to be processed
    unproccessed_queue: MessageQueue,

    /// Queue of message pointers have been processed
    proccessed_queue: MessageQueue,

    /// Number of messages processed by the node
    processed_messages_count: u128,

    /// A transaction map of K transaction_id, V origin_id used to correlate messages
    /// to the senders and receivers
    transaction_map: std.AutoHashMap(uuid.Uuid, u128),

    /// Map of all active connections
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    /// Mutex for the connections_map ensuring that connections are added/removed atomically
    connections_mutex: std.Thread.Mutex,

    /// Workers used to handle messages
    workers: std.ArrayList(*Worker),

    /// Index in the workers array of the last worker to be given a message to process
    worker_round_robin_index: usize = 0,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Self {
        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        const cpuCount = try std.Thread.getCpuCount();

        // TODO: there should be a validate method on the config to ensure that all the fields
        // are actually valid and not hot garb
        assert(config.worker_thread_count >= 1);
        assert(config.worker_thread_count <= cpuCount);

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .running = false,
            .config = config,
            .listener_socket = undefined,
            .io = undefined,
            .message_pool = undefined,
            .unproccessed_queue = MessageQueue.new(constants.queue_size_max),
            .proccessed_queue = MessageQueue.new(constants.queue_size_max),
            .processed_messages_count = 0,
            .accept_submitted = false,
            .accept_completion = accept_completion,
            .transaction_map = std.AutoHashMap(uuid.Uuid, u128).init(allocator),
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .connections_mutex = std.Thread.Mutex{},
            .workers = try std.ArrayList(*Worker).initCapacity(allocator, config.worker_thread_count),
            .worker_round_robin_index = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.accept_completion);
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var conn_iter = self.connections.valueIterator();
        while (conn_iter.next()) |conn| {
            conn.*.deinit();
            self.allocator.destroy(conn.*);
        }

        for (self.workers.items) |worker| {
            worker.close();
        }

        self.connections.deinit();
        self.transaction_map.deinit();
        self.workers.deinit();
    }

    pub fn run(self: *Self) !void {
        if (self.running) @panic("already running");
        self.running = true;
        defer self.running = false;

        // initialize all of the workers
        for (0..self.config.worker_thread_count) |i| {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);

            worker.* = Worker.new(@intCast(i), constants.queue_size_default);
            try self.workers.append(worker);

            const th = try std.Thread.spawn(.{}, Worker.run, .{worker});
            th.detach();
        }

        var io = try IO.init(8, 0);
        defer io.deinit();

        var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = message_pool_gpa.deinit();
        const message_pool_allocator = message_pool_gpa.allocator();

        var message_pool = try MessagePool.init(message_pool_allocator, constants.message_pool_max_size);
        defer message_pool.deinit();

        // initialize the listener socket
        const address = try std.net.Address.parseIp(self.config.host, self.config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const listener_socket = try posix.socket(address.any.family, socket_type, protocol);
        // ensure the socket gets closed
        defer posix.close(listener_socket);

        // add options to listener socket
        try posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.bind(listener_socket, &address.any, address.getOsSockLen());
        try posix.listen(listener_socket, 128);
        log.info("listening on {s}:{d}", .{ self.config.host, self.config.port });

        // NOTE: set everything that is required for running!
        self.io = &io;
        self.message_pool = &message_pool;
        self.listener_socket = listener_socket;

        var timer = try std.time.Timer.start();
        defer timer.reset();
        var start: u64 = 0;
        var printed = false;

        while (true) {
            if (start == 0 and self.processed_messages_count >= 10) {
                timer.reset();
                start = timer.read();
            }
            if (self.processed_messages_count >= 10) {
                const now = timer.read();
                const elapsed_seconds = (now - start) / std.time.ns_per_s;

                if (elapsed_seconds % 5 == 0) {
                    if (!printed) {
                        log.debug("processed {} messages in {}s", .{ self.processed_messages_count, elapsed_seconds });
                        printed = true;
                    }
                } else {
                    printed = false;
                }
            }

            try self.tick();
            try self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms);
        }
    }

    fn tick(self: *Self) !void {
        if (!self.accept_submitted) {
            self.io.accept(*Node, self, Node.onAccept, self.accept_completion, self.listener_socket);
            self.accept_submitted = true;
        }

        // lock the connections. If we can't lock the connections that means that there is some other process using it
        if (self.connections_mutex.tryLock()) {
            defer self.connections_mutex.unlock();

            var conn_iter = self.connections.iterator();

            while (conn_iter.next()) |entry| {
                const conn = entry.value_ptr.*;

                // remove this connection if it is already closed
                if (conn.state == .closed) {
                    conn.deinit();
                    self.allocator.destroy(conn);
                    _ = self.connections.remove(entry.key_ptr.*);
                    continue;
                }

                try conn.tick();

                if (conn.inbox.count > 0) {
                    if (conn.inbox.count + self.unproccessed_queue.count < self.unproccessed_queue.max_size) {
                        // take all the messages in the inbox
                        self.unproccessed_queue.concatenate(&conn.inbox);

                        // clear the inbox completely
                        conn.inbox.clear();
                    } else {
                        while (conn.inbox.dequeue()) |message| {
                            // ensure that there is only a single reference to this message prior to processing it.
                            // if this isn't the case that means that we have an error somewhere on the data injestion
                            // pipeline.
                            assert(message.ref_count == 1);
                            try self.unproccessed_queue.enqueue(message);
                        }
                    }
                }
            }
        }

        // TODO: figure out how many messages are to be processed
        // TODO: loop over the workers
        //      - take any processed messages
        //      - distribute recv messages to workers
        // TODO: workers send processed messages to the node
        // TODO: node routes messages to connections

        // lets say there are 500 unproccessed messages
        // we take the number of worker threads available (24)
        // and divide the messages in an equal amount
        // quotient 500 / 24 = 480
        // remainder 500 % 24 = 20
        // quotient / 24 = 24 messages per worker
        // since we have a remainder in this case, we should give the remainder to the next thread as well

        // starting at the index we last left on
        // workers = [1,2,3,4,5, ...];
        // const worker = workers[last_index];
        // worker.lock();

        // lets just actually concatenate the lists together
        if (self.unproccessed_queue.count > 0) {
            // just make sure we didn't bork this index somehow
            assert(self.worker_round_robin_index >= 0 and self.worker_round_robin_index < self.workers.items.len);

            const worker = self.workers.items[self.worker_round_robin_index];
            worker.mutex.lock();
            defer worker.mutex.unlock();

            if (self.worker_round_robin_index + 1 < self.workers.items.len) {
                self.worker_round_robin_index += 1;
            } else {
                self.worker_round_robin_index = 0;
            }

            worker.unprocessed_messages.concatenate(&self.unproccessed_queue);
            self.unproccessed_queue.clear();

            // the worker now has work to do
            worker.state = .busy;
        }

        // gather up messages from the workers
        for (self.workers.items) |worker| {
            if (worker.processed_messages.count > 0) {
                worker.mutex.lock();
                defer worker.mutex.unlock();

                self.proccessed_queue.concatenate(&worker.processed_messages);
                worker.processed_messages.clear();

                if (worker.unprocessed_messages.count == 0) worker.state = .idle;
            }
        }

        // const old_processed_messages_count = self.processed_messages_count;

        while (self.proccessed_queue.dequeue()) |message| {
            self.processed_messages_count += 1;

            message.deref();
            self.message_pool.destroy(message);
            // log.debug("message {any}", .{message});
        }

        // if (old_processed_messages_count < self.processed_messages_count) {
        //     log.debug("processed messages count {}", .{self.processed_messages_count});
        // }

        // // work for a maximum of {time} before releasing control back to the main loop
        // const processing_deadline = std.time.nanoTimestamp() + (5 * std.time.ns_per_ms);
        //
        // // ensure that we have not exceeded the processing deadline
        // while (processing_deadline >= std.time.nanoTimestamp()) {
        //
        //     // if there are no items to process, then do nothing
        //     while (self.recv_queue.dequeue()) |message| {
        //         self.processed_messages_count += 1;
        //         assert(message.ref_count == 1);
        //
        //         // TODO: a timeout should be passed to handleMessage just in case?
        //         // try self.handleMessage(message);
        //         message.deref();
        //
        //         // assert(message.ref_count == 0);
        //
        //         // once the message has been handled, we should deref the message
        //         if (message.ref_count == 0) {
        //             self.message_pool.destroy(message);
        //         }
        //
        //         // TODO: we should check that messages aren't getting stuck in the system. I think that
        //         // having a timestamp on each message as to when it was created might be an easy way to check
        //         // but then you get into crazy time syncing problems
        //     } else {
        //         return;
        //     }
        // } else {
        //     log.warn("unable to process all messages before processing_deadline. remaining messages in processing_queue {d}", .{self.recv_queue.count});
        // }
    }

    pub fn handleMessage(self: *Self, message: *Message) !void {
        defer self.processed_messages_count += 1;
        defer message.deref();
        switch (message.headers.message_type) {
            .ping => {
                // cast the headers into the correct headers type
                const ping_headers: *const Ping = message.headers.intoConst(.ping).?;

                // drop this message if we have no idea who to end a pong back to
                if (ping_headers.origin_id == 0) {
                    log.warn("received ping message without an origin_id", .{});
                    return;
                }

                // create a pong message
                var pong = try self.message_pool.create();
                pong.* = Message.new();
                pong.headers.message_type = .pong;
                var pong_headers: *Pong = pong.headers.into(.pong).?;
                pong_headers.transaction_id = ping_headers.transaction_id;
                pong_headers.error_code = .ok;

                // make a reference to the new pong message
                pong.ref();

                // add the message to the outbox queue of the connection
                if (self.connections.get(ping_headers.origin_id)) |conn| {
                    // try to have the message enqueued in the outbox
                    conn.outbox.enqueue(pong) catch |err| switch (err) {
                        error.QueueFull => {
                            log.err("dropping message {any}", .{err});
                            pong.deref();
                            self.message_pool.destroy(pong);
                        },
                        else => unreachable,
                    };
                } else {
                    log.err("could not enqueue pong message", .{});
                    pong.deref();
                    self.message_pool.destroy(pong);
                }
            },
            .pong => {
                log.debug("received pong {any}", .{message.headers.message_type});
            },
            .request => {
                // cast the headers into the correct headers type
                const req_headers: *const Request = message.headers.intoConst(.request).?;

                // drop this message if we have no idea who to end a pong back to
                if (req_headers.origin_id == 0) {
                    log.warn("received req message without an origin_id", .{});
                    return;
                }
            },
            // .reply => {},
            // .accept => {},
            else => |t| {
                log.err("received unhandled message type {any}", .{t});
                @panic("reach unrecoverable state");
            },
        }
    }

    // Callback functions
    pub fn onAccept(self: *Self, comp: *IO.Completion, res: IO.AcceptError!posix.socket_t) void {
        _ = comp;

        // FIX: Should handle the case where we are unable to accept the socket
        const socket = res catch |err| {
            log.err("could not accept connection {any}", .{err});
            unreachable;
        };

        self.accept_submitted = false;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        std.debug.print("accepted: socket {any}\n", .{socket});

        const conn = self.allocator.create(Connection) catch unreachable;
        conn.* = Connection.init(self.message_pool, self.io, socket, self.allocator);
        errdefer conn.state = .close;

        const conn_id = uuid.v7.new();
        self.connections.put(conn_id, conn) catch unreachable;

        conn.id = conn_id;

        // put an accept message as the first message to be sent back
        // conn.outbox.enqueue
        const accept_message = self.message_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            conn.state = .close;
            return;
        };

        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.id;

        conn.outbox.enqueue(accept_message) catch unreachable;

        log.info("accepted a connection {}", .{conn.id});
    }
};

test "node does node things" {
    // const allocator = std.testing.allocator;
    //
    // var io = try IO.init(8, 0);
    // defer io.deinit();
    //
    // var message_bus = try MessageBus.init(allocator, &io);
    // defer message_bus.deinit();
    //
    // var node = try Node.init(allocator, .{}, &io, &message_bus);
    // defer node.deinit();
    //
    // try node.tick();
}
