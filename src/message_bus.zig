const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.MessageBus);

const uuid = @import("uuid");

const constants = @import("./constants.zig");

const Message = @import("./message.zig").Message;
const Accept = @import("./message.zig").Accept;
const Request = @import("./message.zig").Request;
const Reply = @import("./message.zig").Reply;
const Ping = @import("./message.zig").Ping;
const Pong = @import("./message.zig").Pong;
const MessageType = @import("./message.zig").MessageType;

const Connection = @import("./connection.zig").Connection;

const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;
const MessagePool = @import("./message_pool.zig").MessagePool;
const IO = @import("./io.zig").IO;

pub const MessageBus = struct {
    /// identifier of the node
    id: uuid.Uuid,

    /// Shared reference to IO
    io: *IO,

    /// Queue of message pointers that still need to be processed. Once the message has been
    /// processed, the message pointer should be removed from the queue and added
    /// back to the free_queue
    processing_queue: MessageQueue,

    /// Reference to the global message pool
    message_pool: *MessagePool,

    /// A transaction map of K transaction_id, V origin_id used to correlate messages
    /// to the senders and receivers
    transaction_map: std.AutoHashMap(uuid.Uuid, u128),

    /// Map of all active connections
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    /// Mutex for the connections_map ensuring that connections are added/removed atomically
    connections_mutex: std.Thread.Mutex,

    /// A map of connections connected to the message bus
    processed_messages_count: u128,

    /// allocator used for connections and transactions
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, io: *IO, message_pool: *MessagePool) !MessageBus {
        return MessageBus{
            .id = uuid.v7.new(),
            .io = io,
            .message_pool = message_pool,
            .processing_queue = MessageQueue.new(constants.queue_size_max),
            .processed_messages_count = 0,
            .transaction_map = std.AutoHashMap(uuid.Uuid, u128).init(allocator),
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .connections_mutex = std.Thread.Mutex{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MessageBus) void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var conn_iter = self.connections.valueIterator();
        while (conn_iter.next()) |conn| {
            conn.*.deinit();
            self.allocator.destroy(conn.*);
        }

        self.connections.deinit();
        self.transaction_map.deinit();
    }

    pub fn handleMessage(self: *MessageBus, message: *Message) !void {
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

    // maybe this should return the connection id?
    pub fn addConnection(self: *MessageBus, socket: posix.socket_t) !void {
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
        const accept_message = try self.message_pool.create();
        errdefer self.message_pool.destroy(accept_message);

        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.id;

        try conn.outbox.enqueue(accept_message);

        log.info("accepted a connection {}", .{conn.id});
    }

    pub fn tick(self: *MessageBus) !void {
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // const start = timer.read();
        // defer log.debug("self.processed_messages_count {d}", .{self.processed_messages_count});
        // defer log.debug("free messages {d}", .{self.message_pool.unassigned_queue.count});

        // defer {
        //     const end = timer.read();
        //     // log.debug("tick took {d}ns", .{(end - start)});
        //     log.debug("tick took {d}us", .{(end - start) / std.time.ns_per_us});
        // }
        // defer {
        //     const total_time = (timer.read() - start) / std.time.ns_per_us;
        //     if (total_time > 5) {
        //         log.debug("tick took {d}us", .{total_time});
        //     }
        // }

        // try to lock the connections so that only this method can modify the existing list of connections
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
                    if (conn.inbox.count + self.processing_queue.count < self.processing_queue.max_size) {
                        // take all the messages in the inbox
                        self.processing_queue.concatenate(&conn.inbox);

                        // clear the inbox completely
                        conn.inbox.clear();
                    } else {
                        while (conn.inbox.dequeue()) |message| {
                            try self.processing_queue.enqueue(message);
                        }
                    }
                }
            }
        }

        // work for a maximum of {time} before releasing control back to the main thread
        const processing_deadline = std.time.nanoTimestamp() + (5 * std.time.ns_per_ms);

        // ensure that we have not exceeded the processing deadline
        while (processing_deadline >= std.time.nanoTimestamp()) {
            // if there are no items to process, then do nothing
            while (self.processing_queue.dequeue()) |message| {
                assert(message.ref_count == 1);

                // TODO: a timeout should be passed to handleMessage just in case?
                try self.handleMessage(message);

                assert(message.ref_count == 0);

                // once the message has been handled, we should deref the message
                if (message.ref_count == 0) {
                    self.message_pool.destroy(message);
                }

                // TODO: we should check that messages aren't getting stuck in the system. I think that
                // having a timestamp on each message as to when it was created might be an easy way to check
                // but then you get into crazy time syncing problems
            } else {
                return;
            }
        } else {
            log.warn("unable to process all messages before processing_deadline. remaining messages in processing_queue {d}", .{self.processing_queue.count});
        }
    }
};
