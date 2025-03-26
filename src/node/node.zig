const std = @import("std");
const posix = std.posix;
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);

const uuid = @import("uuid");
const constants = @import("../constants.zig");

const Acceptor = @import("./acceptor.zig").Acceptor;
const Broker = @import("./broker.zig").Broker;
const Worker = @import("./worker.zig").Worker;
const IO = @import("../io.zig").IO;

const TopicManager = @import("./topic_manager.zig").TopicManager;
const Message = @import("../protocol/message.zig").Message;

// Datastructures
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;
const Channel = @import("../data_structures/channel.zig").Channel;

/// static configuration used to configure the node
pub const NodeConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 8000,
    worker_threads: u32 = 1,
    message_pool_capacity: u32 = constants.message_pool_max_capacity,
};

const NodeState = enum {
    running,
    close,
    closed,
};

pub const Node = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: NodeConfig,
    id: uuid.Uuid,
    io: *IO,
    message_pool: *MessagePool,
    tcp_acceptor: *Acceptor,
    broker: *Broker,
    workers: *std.AutoHashMap(u32, *Worker),
    topic_manager: *TopicManager,
    state: NodeState,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Self {
        const node_id = uuid.v7.new();

        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, config.message_pool_capacity);
        errdefer message_pool.deinit();

        const tcp_acceptor = try allocator.create(Acceptor);
        errdefer allocator.destroy(tcp_acceptor);

        tcp_acceptor.* = try Acceptor.init(allocator, .{ .host = config.host, .port = config.port });
        errdefer tcp_acceptor.deinit();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const topic_manager = try allocator.create(TopicManager);
        errdefer allocator.destroy(topic_manager);

        topic_manager.* = TopicManager.init(allocator, message_pool);
        errdefer topic_manager.deinit();

        const broker = try allocator.create(Broker);
        errdefer allocator.destroy(broker);

        broker.* = try Broker.init(allocator, message_pool, topic_manager, .{});
        errdefer broker.deinit();

        const workers = try allocator.create(std.AutoHashMap(u32, *Worker));
        errdefer allocator.destroy(workers);

        workers.* = std.AutoHashMap(u32, *Worker).init(allocator);
        errdefer workers.deinit();

        for (0..config.worker_threads) |i| {
            // initialize a new worker
            const worker = try allocator.create(Worker);
            errdefer allocator.destroy(worker);

            worker.* = try Worker.init(allocator, broker, message_pool, topic_manager, .{
                .node_id = node_id,
                .id = @intCast(i),
            });
            errdefer workers.deinit();

            try workers.put(worker.config.id, worker);
            errdefer _ = workers.remove(worker.config.id);
        }

        return Self{
            .allocator = allocator,
            .broker = broker,
            .config = config,
            .id = node_id,
            .io = io,
            .message_pool = message_pool,
            .tcp_acceptor = tcp_acceptor,
            .state = .closed,
            .workers = workers,
            .topic_manager = topic_manager,
        };
    }

    pub fn deinit(self: *Self) void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_ptr| {
            const worker = worker_ptr.*;

            worker.deinit();
            self.allocator.destroy(worker);
        }

        self.broker.deinit();
        self.message_pool.deinit();
        self.tcp_acceptor.deinit();
        self.io.deinit();
        self.workers.deinit();
        self.topic_manager.deinit();

        // destroy
        self.allocator.destroy(self.workers);
        self.allocator.destroy(self.broker);
        self.allocator.destroy(self.message_pool);
        self.allocator.destroy(self.tcp_acceptor);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.topic_manager);
    }

    pub fn run(self: *Self) !void {
        assert(self.state == .closed);
        self.state = .running;

        // have the tcp_acceptor start listening for new connections
        try self.tcp_acceptor.listen();
        defer self.tcp_acceptor.close();

        // spawn all worker threads based on config
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_ptr| {
            const worker = worker_ptr.*;
            var ready_chan = Channel(anyerror!bool).init();

            const thread = try std.Thread.spawn(.{}, Worker.run, .{ worker, &ready_chan });
            thread.detach();

            const res = ready_chan.receive();
            _ = try res;
        }

        var start: u64 = 0;
        var timer = try std.time.Timer.start();
        defer timer.reset();
        var printed: bool = false;
        var printed_at: u64 = 0;
        var messages_processed_count_since_last_print: u128 = 0;

        // FIX: This is a spinlock and very no bueno because we end up idling at 100% CPU. Need to figure out a better way to loop when there is new work.
        //  1. a Possible solution is to add a channel that all the workers use to notify that there is new work. I think
        //      that this could be tricky on the whole timing front though.
        while (true) {
            switch (self.state) {
                .running => {
                    // check if there are new sockets to add to the collection!
                    if (start == 0 and self.broker.processed_messages_count > 0) {
                        timer.reset();
                        start = timer.read();
                    }

                    if (self.broker.processed_messages_count > 0) {
                        const now = timer.read();
                        const elapsed_seconds = (now - start) / std.time.ns_per_s;
                        const time_since_last_print = (now - printed_at) / std.time.ns_per_s;

                        if (time_since_last_print >= 1) {
                            if (!printed) {
                                const processed_messages_count = self.broker.processed_messages_count;
                                const free_messages = self.broker.message_pool.available();

                                std.debug.print("duration {}s, processed messages: {}, processed delta {}, free messages {}\n", .{
                                    elapsed_seconds,
                                    processed_messages_count,
                                    processed_messages_count - messages_processed_count_since_last_print,
                                    free_messages,
                                });

                                messages_processed_count_since_last_print = processed_messages_count;

                                printed = true;
                                printed_at = now;
                            }
                        } else {
                            printed = false;
                        }
                    }

                    try self.maybeAddConnection();
                    // try self.topic_manager.tick();

                    try self.broker.tick();

                    // std.time.sleep(1 * std.time.ns_per_ms);
                    // NOTE: This is dumb, since there are no IO operations happening on this
                    // thread. They only happen on the TCP Acceptor and workers. This should
                    // instead be simpler.
                    try self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms);
                },
                .close => {
                    workers_iter = self.workers.valueIterator();
                    while (workers_iter.next()) |worker_ptr| {
                        const worker = worker_ptr.*;
                        worker.close();
                    }
                    // once all the workers are closed, we can just close the node
                    self.state = .closed;
                },
                .closed => return,
            }

            // try and accept new TCP connections
            // try broker.tick();
            // check for new socket connections
            // try self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms);
        }
    }

    pub fn close(self: *Self) void {
        if (self.state == .closed) return;
        if (self.state == .running) self.state = .close;

        // wait for the node to stop
        var i: usize = 0;
        while (self.state != .closed) : (i += 1) {
            log.debug("close node attempt - {}", .{i});

            std.time.sleep(1 * std.time.ns_per_ms);
        }
    }

    fn maybeAddConnection(self: *Self) !void {
        // check if there are new sockets on the tcp_acceptor
        if (self.tcp_acceptor.sockets.items.len > 0) {
            self.tcp_acceptor.mutex.lock();
            defer self.tcp_acceptor.mutex.unlock();

            while (self.tcp_acceptor.sockets.pop()) |socket| {
                try self.addConnection(socket);
            }
        }
    }

    fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // find the worker with the least number of connections
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

        try worker.addConnection(socket);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var node = try Node.init(allocator, .{
        .port = 9753,
        .host = "127.0.0.1",
        .worker_threads = 1,
        .message_pool_capacity = 10,
    });
    defer node.deinit();

    const closeNode = struct {
        fn close(n: *Node) !void {
            std.time.sleep(5 * std.time.ms_per_s);
            n.close();
        }
    }.close;

    const thread = try std.Thread.spawn(.{}, closeNode, .{&node});
    try node.run();
    thread.join();
}
