const std = @import("std");
const posix = std.posix;
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Bus);

const uuid = @import("uuid");

const constants = @import("../constants.zig");
const Message = @import("../message.zig").Message;

const Request = @import("../message.zig").Request;
const Reply = @import("../message.zig").Reply;
const Ping = @import("../message.zig").Ping;
const Pong = @import("../message.zig").Pong;
const Accept = @import("../message.zig").Accept;

const Worker = @import("./worker.zig").Worker;
const Connection = @import("../connection.zig").Connection;

// const Worker = @import("./worker.zig").Worker;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const MessagePool = @import("../message_pool.zig").MessagePool;
const ProtocolError = @import("../errors.zig").ProtocolError;
const Service = @import("./service.zig").Service;
const ServiceManager = @import("./service.zig").ServiceManager;
const IO = @import("../io.zig").IO;

pub const Bus = struct {
    const Self = @This();

    node_id: uuid.Uuid,
    allocator: std.mem.Allocator,
    message_pool: *MessagePool,
    workers: std.AutoHashMap(u32, *Worker),
    unprocessed_messages_queue: *MessageQueue,
    unprocessed_messages_queue_mutex: std.Thread.Mutex,
    processed_messages_count: u128,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    inboxes: std.AutoHashMap(uuid.Uuid, *MessageQueue),
    outboxes: std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)),

    pub fn init(
        node_id: uuid.Uuid,
        worker_thread_count: usize,
        allocator: std.mem.Allocator,
    ) !Self {
        // initialize the message_pool first because it is fixed length
        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, constants.message_pool_max_size);
        errdefer message_pool.deinit();

        const unprocessed_messages_queue = try allocator.create(MessageQueue);
        errdefer allocator.destroy(unprocessed_messages_queue);

        unprocessed_messages_queue.* = MessageQueue.new(constants.message_queue_capacity_max);

        var bus = Self{
            .node_id = node_id,
            .allocator = allocator,
            .message_pool = message_pool,
            .workers = std.AutoHashMap(u32, *Worker).init(allocator),
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .unprocessed_messages_queue = unprocessed_messages_queue,
            .unprocessed_messages_queue_mutex = std.Thread.Mutex{},
            .processed_messages_count = 0,
            .inboxes = std.AutoHashMap(uuid.Uuid, *MessageQueue).init(allocator),
            .outboxes = std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)).init(allocator),
        };

        // spawn a worker per core
        const cpu_cores = try std.Thread.getCpuCount();
        if (worker_thread_count > cpu_cores) return error.RequestThreadsExceedAvailableCores;
        assert(worker_thread_count >= 1);

        for (0..worker_thread_count) |i| {
            const worker = try allocator.create(Worker);
            errdefer allocator.destroy(worker);

            worker.* = try Worker.init(
                bus.node_id,
                @intCast(i + 1),
                allocator,
                &bus,
                bus.message_pool,
            );

            try bus.workers.put(@intCast(i + 1), worker);

            const th = try std.Thread.spawn(.{}, Worker.run, .{worker});
            th.detach();
        }

        // wait 10ms for threads to spin up
        // FIX: this should actually use a condition or something
        std.time.sleep(10 * std.time.ns_per_ms);

        return bus;
    }

    pub fn deinit(self: *Self) void {
        // go through every worker and shut them down
        var workers_iter = self.workers.iterator();
        self.workers.lockPointers();

        while (workers_iter.next()) |entry| {
            entry.value_ptr.*.close();
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            _ = self.workers.remove(entry.key_ptr.*);
        }

        self.workers.unlockPointers();
        self.workers.deinit();

        var inboxes_iter = self.inboxes.valueIterator();
        self.inboxes.lockPointers();
        while (inboxes_iter.next()) |inbox_ptr| {
            while (inbox_ptr.*.dequeue()) |message| {
                message.deref();
                if (message.ref_count == 0) {
                    self.message_pool.destroy(message);
                }
            }
        }
        self.inboxes.unlockPointers();

        var outboxes_iter = self.outboxes.valueIterator();
        self.outboxes.lockPointers();
        while (outboxes_iter.next()) |inbox_ptr| {
            inbox_ptr.*.deinit();
        }
        self.outboxes.unlockPointers();

        self.inboxes.deinit();
        self.outboxes.deinit();
        self.message_pool.deinit();

        self.allocator.destroy(self.message_pool);
        self.allocator.destroy(self.unprocessed_messages_queue);

        // NOTE: I don't know how I feel about having double refs to connections but it is really feeling
        // like this is probably the easiest way for the bus to directly push to connections. The alternative
        // is to have the workers pull from the message bus when they are ready
        // self.connections.deinit();
    }

    pub fn tick(self: *Self) !void {
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // const start = timer.read();
        // const messages_already_processed: u128 = self.processed_messages_count;
        // defer {
        //     const end = timer.read();
        //     const took = ((end - start) / std.time.ns_per_us);
        //
        //     log.debug("tick: {d:6}us, processed_tick: {d:6} processed_total: {d:8}, free: {d:6}, ", .{
        //         took,
        //         self.processed_messages_count - messages_already_processed,
        //         self.processed_messages_count,
        //         self.message_pool.unassigned_queue.count,
        //     });
        // }

        var worker_iter = self.workers.valueIterator();
        while (worker_iter.next()) |worker_ptr| {
            const worker = worker_ptr.*;
            if (worker.inbox.count > 0) {
                worker.inbox_mutex.lock();
                defer worker.inbox_mutex.unlock();

                // if there is enough space to accomodate the messages in the worker queue
                if (self.unprocessed_messages_queue.count + worker.inbox.count < self.unprocessed_messages_queue.capacity) {
                    // take the workers messages and add them to the bus' inbox
                    self.unprocessed_messages_queue.concatenate(&worker.inbox);
                    worker.inbox.clear();
                } else {
                    log.info("skipping worker {} due to bus.inbox being too full", .{worker.id});
                    // just continue and try to process some messages
                    break;
                }
            }
        }

        if (self.unprocessed_messages_queue.count == 0) return;

        self.unprocessed_messages_queue_mutex.lock();
        defer self.unprocessed_messages_queue_mutex.unlock();

        while (self.unprocessed_messages_queue.dequeue()) |message| {
            defer self.processed_messages_count += 1;
            assert(message.ref_count == 1);

            try self.handleMessage(message);

            // once the message has been handled, we should deref the message
            if (message.ref_count == 0) {
                self.message_pool.destroy(message);
            }
        }
    }

    fn handleMessage(self: *Self, message: *Message) !void {
        defer {
            self.processed_messages_count += 1;
            message.deref();
        }

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
                errdefer self.message_pool.destroy(pong);

                pong.* = Message.new();
                pong.headers.message_type = .pong;
                var pong_headers: *Pong = pong.headers.into(.pong).?;
                pong_headers.transaction_id = ping_headers.transaction_id;
                pong_headers.error_code = .ok;

                // make a reference to the new pong message
                pong.ref();
                errdefer pong.deref();

                if (self.outboxes.get(ping_headers.origin_id)) |outbox| {
                    try outbox.enqueue(pong);
                } else {
                    log.err("connection outbox not found {}", .{ping_headers.origin_id});

                    pong.deref();
                    self.message_pool.destroy(pong);
                }
            },
            else => unreachable,
        }
    }

    pub fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // NOTE: this isn't an optimal solution. Instead we should redistribute connections evenly
        // to all workers to ensure that we are spreading the connection load.

        // figure out what worker should get the connection based on how many connections each has
        var worker_iter = self.workers.valueIterator();
        var worker_with_min_connections: ?*Worker = null;
        var min_connections: u32 = 0;

        while (worker_iter.next()) |worker_ptr| {
            const worker = worker_ptr.*;
            if (worker_with_min_connections == null) {
                worker_with_min_connections = worker;
                min_connections = worker.connections.count();
                continue;
            }

            if (worker.connections.count() < min_connections) {
                worker_with_min_connections = worker;
                min_connections = worker.connections.count();
            }
        }

        if (worker_with_min_connections == null) unreachable;
        const worker = worker_with_min_connections.?;

        log.debug("connection added to worker {}", .{worker.id});

        // create the inbox and outbox for the connection
        const inbox = try self.allocator.create(MessageQueue);
        errdefer self.allocator.destroy(inbox);

        const outbox = try self.allocator.create(RingBuffer(*Message));
        errdefer self.allocator.destroy(outbox);

        inbox.* = MessageQueue.new(constants.message_queue_capacity_default);

        outbox.* = try RingBuffer(*Message).init(self.allocator, 5_000);
        errdefer outbox.deinit();

        const conn_id = uuid.v7.new();

        // add the inbox and outbox to the map
        try self.inboxes.put(conn_id, inbox);
        errdefer _ = self.inboxes.remove(conn_id);

        try self.outboxes.put(conn_id, outbox);
        errdefer _ = self.outboxes.remove(conn_id);

        try worker.addConnection(conn_id, socket, inbox, outbox);
    }
};

test "bus tick" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 500);
    defer message_pool.deinit();

    var bus = try Bus.init(uuid.v7.new(), allocator);
    defer bus.deinit();
}
