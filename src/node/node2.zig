const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);
const posix = std.posix;

const uuid = @import("uuid");

const Worker = @import("./worker2.zig").Worker;
const Listener = @import("./listener.zig").Listener;
const ConnectionConfig = @import("./listener.zig").ConnectionConfig;
const ListenerConfig = @import("./listener.zig").ListenerConfig;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;

pub const NodeConfig = struct {
    const Self = @This();

    worker_threads: usize = 3,
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 5_000,
    listener_configs: ?[]const ListenerConfig = null,
    outbound_configs: ?[]const ConnectionConfig = null, // a list of hosts this node will attempt to automatically connect to

    pub fn validate(self: Self) ?[]const u8 {
        const cpu_core_count = std.Thread.getCpuCount() catch {
            const msg = "could not getCpuCount";
            @panic(msg);
        };
        if (self.worker_threads > cpu_core_count) return "`worker_threads` exceeds cpu core count";
        if (self.max_connections > 5_000) return "`max_connections` exceeds arbitrary limit";
        if (self.memory_pool_capacity > 500_000) return "`memory_pool_capacity` exceeds arbitrary limit";
        if (self.listener_configs) |listener_configs| {
            if (listener_configs.len == 0) return "`listeners` is non null but contains no entries";
            for (listener_configs) |listener_config| {
                switch (listener_config.transport) {
                    .tcp => {},
                }
            }
        }
        if (self.outbound_configs) |remote_configs| {
            if (remote_configs.len == 0) return "`remotes` is non null but contains no entries";
            for (remote_configs) |remote_config| {
                switch (remote_config.transport) {
                    .tcp => {},
                }
            }
        }

        return null;
    }
};

const NodeState = enum {
    running,
    closing,
    closed,
};

pub const Node = struct {
    const Self = @This();

    id: uuid.Uuid,
    allocator: std.mem.Allocator,
    config: NodeConfig,
    listeners: *std.AutoHashMap(usize, *Listener),
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    state: NodeState,
    workers: *std.AutoHashMap(usize, *Worker),
    memory_pool: *MemoryPool(Message),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Self {
        if (config.validate()) |err_message| {
            log.err("invalid config: {s}", .{err_message});
            return error.InvalidConfig;
        }

        // ensure that this node is not spawning a crazy amount of threads
        const cpu_count = try std.Thread.getCpuCount();
        assert(config.worker_threads < cpu_count);

        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const workers = try allocator.create(std.AutoHashMap(usize, *Worker));
        errdefer allocator.destroy(workers);

        workers.* = std.AutoHashMap(usize, *Worker).init(allocator);
        errdefer workers.deinit();

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        const listeners = try allocator.create(std.AutoHashMap(usize, *Listener));
        errdefer allocator.destroy(listeners);

        listeners.* = std.AutoHashMap(usize, *Listener).init(allocator);
        errdefer listeners.deinit();

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .config = config,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
            .workers = workers,
            .memory_pool = memory_pool,
            .listeners = listeners,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;
            listener.deinit();
            self.allocator.destroy(listener);
        }

        var workers_iterator = self.workers.valueIterator();
        while (workers_iterator.next()) |entry| {
            const worker = entry.*;
            worker.deinit();
            self.allocator.destroy(worker);
        }

        self.listeners.deinit();
        self.workers.deinit();
        self.memory_pool.deinit();

        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.workers);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn connect(self: *Self) !void {
        _ = self;
    }

    pub fn disconnect(self: *Self) void {
        _ = self;
    }

    pub fn start(self: *Self) !void {
        assert(self.state == .closed);

        // Start the workers
        try self.initializeWorkers();
        try self.spawnWorkerThreads();

        // Start the listeners
        try self.initializeListeners();
        try self.spawnListeners();

        // Start the outbound connections
        try self.initializeOutboundConnections();

        // Start the core thread
        var ready_channel = UnbufferedChannel(bool).new();
        const core_thread = try std.Thread.spawn(.{}, Node.run, .{ self, &ready_channel });
        core_thread.detach();

        _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("core_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn run(self: *Node, ready_channel: *UnbufferedChannel(bool)) void {
        self.state = .running;
        ready_channel.send(true);
        log.info("node {} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("node {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                },
                .closing => {
                    log.info("node {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                self.mutex.lock();
                defer self.mutex.unlock();

                // spin down the workers
                var worker_iterator = self.workers.valueIterator();
                while (worker_iterator.next()) |entry| {
                    const worker = entry.*;

                    worker.close();
                }

                // spind down the listeners
                var listener_iterator = self.listeners.valueIterator();
                while (listener_iterator.next()) |entry| {
                    const listener = entry.*;

                    listener.close();
                }

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
    }

    fn tick(self: *Self) !void {
        try self.maybeAddInboundConnections();
        // TODO: try self.processMessages()
    }

    fn initializeWorkers(self: *Self) !void {
        assert(self.workers.count() == 0);

        // initialize `n` connection_workers
        for (0..self.config.worker_threads) |id| {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);

            worker.* = try Worker.init(self.allocator, id, self);
            errdefer worker.deinit();

            try self.workers.put(id, worker);
            errdefer _ = self.workers.remove(id);
        }
    }

    fn spawnWorkerThreads(self: *Self) !void {
        assert(self.workers.count() == self.config.worker_threads);

        var ready_channel = UnbufferedChannel(bool).new();
        // Spawn all of the connection_workers
        var worker_iterator = self.workers.valueIterator();
        while (worker_iterator.next()) |entry| {
            const worker = entry.*;
            assert(worker.state == .closed);

            const worker_thread = try std.Thread.spawn(.{}, Worker.run, .{ worker, &ready_channel });
            worker_thread.detach();

            _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
                log.err("worker_thread spawn timeout", .{});
                self.close();
                return err;
            };
        }
    }

    fn initializeListeners(self: *Self) !void {
        if (self.config.listener_configs) |listener_configs| {
            // we have already validated that the configuration is valid
            for (listener_configs, 0..listener_configs.len) |listener_config, id| {
                switch (listener_config.transport) {
                    .tcp => {
                        const tcp_listener = try self.allocator.create(Listener);
                        errdefer self.allocator.destroy(tcp_listener);

                        tcp_listener.* = try Listener.init(self.allocator, id, .{
                            .host = listener_config.host,
                            .port = listener_config.port,
                        });
                        errdefer tcp_listener.deinit();

                        try self.listeners.put(id, tcp_listener);
                    },
                }
            }
        }
    }

    fn spawnListeners(self: *Self) !void {
        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;

            var ready_channel = UnbufferedChannel(bool).new();

            const listener_thread = try std.Thread.spawn(.{}, Listener.run, .{ listener, &ready_channel });
            listener_thread.detach();

            _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
                log.err("listener_thread spawn timeout", .{});
                self.close();
                return err;
            };
        }
    }

    fn initializeOutboundConnections(self: *Self) !void {
        if (self.config.outbound_configs) |outbound_configs| {
            for (outbound_configs) |outbound_config| {
                switch (outbound_config.transport) {
                    .tcp => {
                        try self.addOutboundConnectionToWorker(outbound_config);
                    },
                }
            }
        }
    }

    fn maybeAddInboundConnections(self: *Self) !void {
        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;

            if (listener.sockets.items.len > 0) {
                listener.mutex.lock();
                defer listener.mutex.unlock();

                // try to add the connections
                while (listener.sockets.pop()) |socket| {
                    try self.addConnectionToNextWorker(socket);
                }
            }
        }
    }

    fn addConnectionToNextWorker(self: *Self, socket: posix.socket_t) !void {
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

        try worker.addInboundConnection(socket);
    }

    fn addOutboundConnectionToWorker(self: *Self, config: ConnectionConfig) !void {
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

        // create the socket
        const address = try std.net.Address.parseIp4(config.host, config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const socket = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(socket);

        try worker.addOutboundConnection(socket, address);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var node = try Node.init(allocator, .{});
    defer node.deinit();
}

test "the api" {
    const allocator = testing.allocator;

    var node = try Node.init(allocator, .{});
    defer node.deinit();

    try node.run();
    defer node.close();

    try node.connect();
    defer node.disconnect();
}
