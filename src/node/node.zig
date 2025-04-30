const std = @import("std");
const posix = std.posix;
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);

const uuid = @import("uuid");
const constants = @import("../constants.zig");

const Acceptor = @import("./acceptor.zig").Acceptor;
const Worker = @import("./worker.zig").Worker;
const IO = @import("../io.zig").IO;
const TopicManager = @import("../pubsub/topic_manager.zig").TopicManager;

const Message = @import("../protocol/message.zig").Message;

// Datastructures
const Channel = @import("../data_structures/channel.zig").Channel;
const RingBuffer = @import("stdx").RingBuffer;

/// static configuration used to configure the node
pub const NodeConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 8000,
    worker_threads: u32 = 1,
    worker_message_pool_capacity: u32 = constants.default_worker_message_pool_capacity,
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
    mutex: std.Thread.Mutex,
    state: NodeState,
    tcp_acceptor: *Acceptor,
    workers: *std.AutoHashMap(u32, *Worker),
    processed_messages_count: u128,
    topic_manager: *TopicManager,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !*Self {
        const tcp_acceptor = try allocator.create(Acceptor);
        errdefer allocator.destroy(tcp_acceptor);

        tcp_acceptor.* = try Acceptor.init(allocator, .{ .host = config.host, .port = config.port });
        errdefer tcp_acceptor.deinit();

        const topic_manager = try allocator.create(TopicManager);
        errdefer allocator.destroy(topic_manager);

        topic_manager.* = TopicManager.init(allocator);
        errdefer topic_manager.deinit();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const workers = try allocator.create(std.AutoHashMap(u32, *Worker));
        errdefer allocator.destroy(workers);

        workers.* = std.AutoHashMap(u32, *Worker).init(allocator);
        errdefer workers.deinit();

        const node = try allocator.create(Node);
        errdefer allocator.destroy(node);

        node.* = Self{
            .allocator = allocator,
            .config = config,
            .id = uuid.v7.new(),
            .io = io,
            .mutex = std.Thread.Mutex{},
            .state = .closed,
            .tcp_acceptor = tcp_acceptor,
            .workers = workers,
            .processed_messages_count = 0,
            .topic_manager = topic_manager,
        };

        for (0..config.worker_threads) |i| {
            // initialize a new worker
            const worker = try allocator.create(Worker);
            errdefer allocator.destroy(worker);

            worker.* = try Worker.init(allocator, node, .{
                .node_id = node.id,
                .id = @intCast(i),
                .message_pool_capacity = config.worker_message_pool_capacity,
            });
            errdefer workers.deinit();

            try workers.put(worker.config.id, worker);
            errdefer _ = workers.remove(worker.config.id);
        }

        return node;
    }

    pub fn deinit(self: *Self) void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_ptr| {
            const worker = worker_ptr.*;

            worker.deinit();
            self.allocator.destroy(worker);
        }

        self.tcp_acceptor.deinit();
        self.io.deinit();
        self.workers.deinit();
        self.topic_manager.deinit();

        // destroy
        self.allocator.destroy(self.workers);
        self.allocator.destroy(self.tcp_acceptor);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.topic_manager);
        self.allocator.destroy(self);
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
                    if (start == 0 and self.processed_messages_count > 0) {
                        timer.reset();
                        start = timer.read();
                    }

                    if (self.processed_messages_count > 0) {
                        const now = timer.read();
                        const elapsed_seconds = (now - start) / std.time.ns_per_s;
                        const time_since_last_print = (now - printed_at) / std.time.ns_per_s;

                        if (time_since_last_print >= 1) {
                            if (!printed) {
                                const processed_messages_count = self.processed_messages_count;

                                log.err("duration {}s, processed messages: {}, processed delta {}\n", .{
                                    elapsed_seconds,
                                    processed_messages_count,
                                    processed_messages_count - messages_processed_count_since_last_print,
                                });

                                messages_processed_count_since_last_print = processed_messages_count;

                                printed = true;
                                printed_at = now;
                            }
                        } else {
                            printed = false;
                        }
                    }

                    try self.tick();
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

    pub fn tick(self: *Self) !void {
        try self.maybeAddConnection();
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
        .worker_message_pool_capacity = 100,
    });
    defer node.deinit();

    const closeNode = struct {
        fn close(n: *Node) !void {
            std.time.sleep(5 * std.time.ms_per_s);
            n.close();
        }
    }.close;

    const thread = try std.Thread.spawn(.{}, closeNode, .{node});
    try node.run();
    thread.join();
}
