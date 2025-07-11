const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const IO = @import("../io.zig").IO;
const Worker = @import("./worker.zig").Worker;
const Listener = @import("./listener.zig").Listener;
const ListenerConfig = @import("./listener.zig").ListenerConfig;
const Remote = @import("./remote.zig").Remote;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const Metrics = @import("./metrics.zig").Metrics;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Connection = @import("../protocol/connection.zig").Connection;

const Publisher = @import("../pubsub/publisher.zig").Publisher;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const Topic = @import("../pubsub/topic.zig").Topic;

// const Publisher = @import("../bus/publisher.zig").Publisher;
// const Subscriber = @import("../bus/subscriber.zig").Subscriber;
// const Bus = @import("../bus/bus.zig").Bus;
// const BusManager = @import("../bus/bus_manager.zig").BusManager;

pub const PingOptions = struct {};

pub const NodeConfig = struct {
    const Self = @This();

    worker_threads: usize = 3,
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 10_000,
    listener_configs: ?[]const ListenerConfig = null,
    outbound_configs: ?[]const OutboundConnectionConfig = null,

    pub fn validate(self: Self) ?[]const u8 {
        const cpu_core_count = std.Thread.getCpuCount() catch {
            const msg = "could not getCpuCount";
            @panic(msg);
        };
        if (self.worker_threads > cpu_core_count) return "NodeConfig `worker_threads` exceeds cpu core count";
        if (self.max_connections > 5_000) return "NodeConfig `max_connections` exceeds arbitrary limit";
        if (self.memory_pool_capacity > 500_000) return "NodeConfig `memory_pool_capacity` exceeds arbitrary limit";

        if (self.listener_configs) |listener_configs| {
            if (listener_configs.len == 0) return "NodeConfig `listener_configs` is non null but contains no entries";
            for (listener_configs) |listener_config| {
                switch (listener_config.transport) {
                    .tcp => {},
                }
            }
        }
        if (self.outbound_configs) |outbound_configs| {
            if (outbound_configs.len == 0) return "NodeConfig `outbound_configs` is non null but contains no entries";
            for (outbound_configs) |outbound_config| {
                switch (outbound_config.transport) {
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
    io: *IO,
    allocator: std.mem.Allocator,
    config: NodeConfig,
    listeners: *std.AutoHashMap(usize, *Listener),
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    state: NodeState,
    workers: *std.AutoHashMap(usize, *Worker),
    connections: *std.AutoHashMap(u128, *Connection),
    memory_pool: *MemoryPool(Message),
    remotes: *std.AutoHashMap(uuid.Uuid, *Remote),
    mutex: std.Thread.Mutex,
    // publishers: std.AutoHashMap(u128, *Publisher),
    // subscribers: std.AutoHashMap(u128, *Subscriber),
    topics: std.StringHashMap(*Topic),
    metrics: Metrics,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Self {
        if (config.validate()) |err_message| {
            log.err("invalid config: {s}", .{err_message});
            return error.InvalidConfig;
        }

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

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

        const connections = try allocator.create(std.AutoHashMap(u128, *Connection));
        errdefer allocator.destroy(connections);

        connections.* = std.AutoHashMap(u128, *Connection).init(allocator);
        errdefer connections.deinit();

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        const remotes = try allocator.create(std.AutoHashMap(uuid.Uuid, *Remote));
        errdefer allocator.destroy(remotes);

        remotes.* = std.AutoHashMap(uuid.Uuid, *Remote).init(allocator);
        errdefer remotes.deinit();

        const listeners = try allocator.create(std.AutoHashMap(usize, *Listener));
        errdefer allocator.destroy(listeners);

        listeners.* = std.AutoHashMap(usize, *Listener).init(allocator);
        errdefer listeners.deinit();

        return Self{
            .id = uuid.v7.new(),
            .io = io,
            .allocator = allocator,
            .config = config,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
            .workers = workers,
            .connections = connections,
            .memory_pool = memory_pool,
            .remotes = remotes,
            .listeners = listeners,
            // .publishers = std.AutoHashMap(u128, *Publisher).init(allocator),
            // .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .topics = std.StringHashMap(*Topic).init(allocator),
            .mutex = std.Thread.Mutex{},
            .metrics = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;
            listener.deinit();
            self.allocator.destroy(listener);
        }

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var workers_iterator = self.workers.valueIterator();
        while (workers_iterator.next()) |entry| {
            const worker = entry.*;
            worker.deinit();
            self.allocator.destroy(worker);
        }

        var remotes_iterator = self.remotes.valueIterator();
        while (remotes_iterator.next()) |entry| {
            const remote = entry.*;
            remote.deinit();
            self.allocator.destroy(remote);
        }

        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;
            topic.deinit();
            self.allocator.destroy(topic);
        }

        self.listeners.deinit();
        self.workers.deinit();
        self.remotes.deinit();
        self.memory_pool.deinit();
        self.io.deinit();
        self.connections.deinit();
        self.topics.deinit();

        self.allocator.destroy(self.io);
        self.allocator.destroy(self.remotes);
        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.workers);
        self.allocator.destroy(self.connections);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn start(self: *Self) !void {
        assert(self.state == .closed);

        // Start the workers
        // try self.initializeWorkers();
        // try self.spawnWorkerThreads();

        // Start the listeners
        try self.initializeListeners();
        try self.spawnListeners();

        // Start the outbound connections
        try self.initializeOutboundConnections();

        // Start the core thread
        var ready_channel = UnbufferedChannel(bool).new();
        const core_thread = try std.Thread.spawn(.{}, Node.run, .{ self, &ready_channel });
        core_thread.detach();

        _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
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
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("node {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;

                    self.io.run_for_ns(100 * std.time.ns_per_us) catch unreachable;
                    // self.io.run_for_ns(10 * std.time.ns_per_ms) catch unreachable;
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

    pub fn connect(self: *Self, config: OutboundConnectionConfig, timeout_ns: i64) !ConnectionHandle {
        const ch = try self.addOutboundConnectionToNextWorker(config);
        const deadline = std.time.nanoTimestamp() + timeout_ns;
        while (deadline > std.time.nanoTimestamp()) {
            if (ch.connection.state == .connected) return ch;

            std.time.sleep(constants.io_tick_ms * std.time.ns_per_ms);
        } else {
            return error.DeadlineExceeded;
        }
    }

    pub fn disconnect(self: *Self, conn: *Connection) void {
        _ = self;
        conn.state = .closing;
    }

    pub fn ping(self: *Self, conn_handle: ConnectionHandle, opts: PingOptions, timeout_ns: i64) !Message {
        _ = opts;
        const ping_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(ping_message);

        ping_message.* = Message.new();
        ping_message.headers.message_type = .ping;
        ping_message.setTransactionId(uuid.v7.new());
        ping_message.ref();
        errdefer ping_message.deref();

        var pong_channel = UnbufferedChannel(*Message).new();

        try self.transactions.put(ping_message.transactionId(), &pong_channel);
        errdefer _ = self.transactions.remove(ping_message.transactionId());

        const deadline = std.time.nanoTimestamp() + timeout_ns;
        if (deadline > std.time.nanoTimestamp()) {
            conn_handle.connection.outbox_mutex.lock();
            defer conn_handle.connection.outbox_mutex.unlock();

            try conn_handle.connection.outbox.enqueue(ping_message);
        }

        const now = std.time.nanoTimestamp();
        const remaining_timeout = deadline - now;
        if (remaining_timeout < 0) return error.Timeout;
        const pong_message = try pong_channel.tryReceive(@intCast(remaining_timeout));

        return pong_message.*;
    }

    fn tick(self: *Self) !void {
        try self.maybeAddInboundConnections();

        const now_ms = std.time.milliTimestamp();
        const difference = now_ms - self.metrics.last_printed_at_ms;
        if (difference > 5_000) {
            const delta = self.metrics.messages_processed - self.metrics.last_messages_processed_printed;
            self.metrics.last_messages_processed_printed = self.metrics.messages_processed;
            self.metrics.last_printed_at_ms = std.time.milliTimestamp();
            log.info("memory_pool.available: {}, messages processed {}, delta {}", .{
                self.memory_pool.available(),
                self.metrics.last_messages_processed_printed,
                delta,
            });
        }

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.*;

            // check if this connection was closed for whatever reason
            if (conn.state == .closed) {
                self.removeConnection(conn);
                continue;
            }

            try conn.tick();
            self.process(conn) catch |err| {
                log.err("could not process connection: {}, err: {any}", .{ conn.connection_id, err });
                continue;
            };
        }

        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;
            try topic.tick();
        }
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

            _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
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

                        tcp_listener.* = try Listener.init(self.allocator, id, listener_config);
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

            _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
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
                        // These connections are not directly handled by a client and therefore
                        // do not need to be handled here. Instead the node will work on them organically
                        _ = try self.addOutboundConnectionToNextWorker(outbound_config);
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
                    try self.addInboundConnection(socket);
                    // try self.addInboundConnectionToNextWorker(socket);
                }
            }
        }
    }

    fn addInboundConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);
        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const default_inbound_connection_config = InboundConnectionConfig{};

        const conn_id = uuid.v7.new();
        connection.* = try Connection.init(
            conn_id,
            self.id,
            .inbound,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
            .{ .inbound = default_inbound_connection_config },
        );
        errdefer connection.deinit();

        connection.state = .connecting;

        // self.connections_mutex.lock();
        // defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, connection);
        errdefer _ = self.connections.remove(conn_id);

        const accept_message = self.memory_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .closing;
            return;
        };
        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.connection_id = conn_id;
        accept_headers.origin_id = self.id;

        try connection.outbox.enqueue(accept_message);

        log.info("node: {} added connection {}", .{ self.id, conn_id });
    }

    fn addInboundConnectionToNextWorker(self: *Self, socket: posix.socket_t) !void {
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

    fn addOutboundConnectionToNextWorker(self: *Self, config: OutboundConnectionConfig) !ConnectionHandle {
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

        const connection = try worker.addOutboundConnection(config);

        const ch = ConnectionHandle{
            .connection = connection,
            .worker = worker,
        };

        return ch;
    }

    fn process(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        while (conn.inbox.dequeue()) |message| {
            self.metrics.messages_processed += 1;
            self.metrics.last_updated_at_ms = std.time.milliTimestamp();
            // defer self.node.processed_messages_count += 1;
            defer {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }
            switch (message.headers.message_type) {
                .accept => {
                    // ensure that this connection is not fully connected
                    assert(conn.state != .connected);

                    switch (conn.connection_type) {
                        .outbound => {
                            assert(conn.connection_id == 0);
                            // An error here would be a protocol error
                            assert(conn.remote_id != message.headers.origin_id);
                            assert(conn.connection_id != message.headers.connection_id);

                            conn.connection_id = message.headers.connection_id;
                            conn.remote_id = message.headers.origin_id;

                            // enqueue a message to immediately convey the node id of this Node
                            message.headers.origin_id = conn.origin_id;
                            message.headers.connection_id = conn.connection_id;

                            message.ref();
                            try conn.outbox.enqueue(message);

                            conn.state = .connected;
                            log.info("outbound_connection - origin_id: {}, connection_id: {}, remote_id: {}", .{
                                conn.origin_id,
                                conn.connection_id,
                                conn.remote_id,
                            });
                        },
                        .inbound => {
                            assert(conn.connection_id == message.headers.connection_id);
                            assert(conn.origin_id != message.headers.origin_id);

                            conn.remote_id = message.headers.origin_id;
                            conn.state = .connected;
                            log.info("inbound_connection - origin_id: {}, connection_id: {}, remote_id: {}", .{
                                conn.origin_id,
                                conn.connection_id,
                                conn.remote_id,
                            });
                        },
                    }
                },
                .ping => {
                    log.info("received ping from origin_id: {}, connection_id: {}", .{
                        message.headers.origin_id,
                        message.headers.connection_id,
                    });
                    // Since this is a `ping` we don't need to do any extra work to figure out how to respond
                    message.headers.message_type = .pong;
                    message.headers.origin_id = self.id;
                    message.headers.connection_id = conn.connection_id;
                    message.setTransactionId(message.transactionId());
                    message.setErrorCode(.ok);

                    assert(message.refs() == 1);

                    if (conn.outbox.enqueue(message)) |_| {} else |err| {
                        log.err("Failed to enqueue message to outbox: {}", .{err});
                        message.deref(); // Undo reference if enqueue fails
                    }
                    message.ref();
                },
                .pong => {
                    log.debug("received pong from origin_id: {}, connection_id: {}", .{
                        message.headers.origin_id,
                        message.headers.connection_id,
                    });
                },
                .publish => {
                    // get the topic by the topicName
                    var topic: *Topic = undefined;
                    if (self.topics.get(message.topicName())) |t| {
                        topic = t;
                    } else {
                        topic = try self.allocator.create(Topic);
                        errdefer self.allocator.destroy(topic);

                        topic.* = try Topic.init(self.allocator, self.memory_pool, message.topicName());
                        errdefer topic.deinit();

                        try self.topics.put(message.topicName(), topic);
                    }

                    const publisher_key = utils.generateKey(message.topicName(), conn.connection_id);
                    var publisher: *Publisher = undefined;
                    if (topic.publishers.get(publisher_key)) |p| {
                        publisher = p;
                    } else {
                        publisher = try self.allocator.create(Publisher);
                        errdefer self.allocator.destroy(publisher);

                        publisher.* = Publisher.new(conn.connection_id, topic);
                        try topic.publishers.put(publisher_key, publisher);
                    }
                    // NOTE: if this connection fails to publish the message it just exits, the more optimal behavior
                    // would instead be to keep that message within the inbox of of the connection and continue working
                    // through the messages
                    assert(message.refs() == 1);
                    publisher.publish(message) catch |err| {
                        log.err("could not publish message: {any}", .{err});
                    };
                },
                .subscribe => {
                    // defer message.deref();
                    log.debug("subscribe message received! topic: {s}", .{message.topicName()});

                    // craft the reply
                    const reply = try self.memory_pool.create();
                    errdefer reply.deref();

                    reply.* = Message.new();

                    reply.headers.message_type = .reply;
                    reply.setTopicName(message.topicName());
                    reply.setTransactionId(message.transactionId());
                    reply.setErrorCode(.ok);
                    reply.ref();
                    defer conn.outbox.enqueue(reply) catch unreachable;

                    {
                        errdefer reply.setErrorCode(.err);

                        // get the topic by the topicName
                        var topic: *Topic = undefined;
                        if (self.topics.get(message.topicName())) |t| {
                            topic = t;
                        } else {
                            topic = try self.allocator.create(Topic);
                            errdefer self.allocator.destroy(topic);

                            topic.* = try Topic.init(self.allocator, self.memory_pool, message.topicName());
                            errdefer topic.deinit();

                            try self.topics.put(message.topicName(), topic);
                        }

                        const subscriber_key = utils.generateKey(message.topicName(), conn.connection_id);
                        var subscriber: *Subscriber = undefined;
                        if (topic.subscribers.get(subscriber_key)) |p| {
                            subscriber = p;
                            // FIX: figure out if this should return an error? Should a connection only be able to subscribe
                            // only once to a topic?
                        } else {
                            subscriber = try self.allocator.create(Subscriber);
                            errdefer self.allocator.destroy(subscriber);

                            subscriber.* = Subscriber.new(conn.connection_id, conn.outbox, topic);
                            try topic.subscribers.put(subscriber_key, subscriber);
                        }
                    }
                },
                .unsubscribe => {
                    log.info("unsubscribing from {s}", .{message.topicName()});
                    // craft the reply
                    const reply = try self.memory_pool.create();
                    errdefer reply.deref();

                    reply.* = Message.new();

                    reply.headers.message_type = .reply;
                    reply.setTopicName(message.topicName());
                    reply.setTransactionId(message.transactionId());
                    reply.setErrorCode(.ok);
                    reply.ref();
                    defer conn.outbox.enqueue(reply) catch unreachable;

                    if (self.topics.get(message.topicName())) |topic| {
                        const subscriber_key = utils.generateKey(message.topicName(), conn.connection_id);
                        if (topic.subscribers.get(subscriber_key)) |subscriber| {
                            self.allocator.destroy(subscriber);
                            assert(topic.subscribers.remove(subscriber_key));
                        }
                    } else {
                        reply.setErrorCode(.err);
                    }
                },
                else => {},
            }
        }

        assert(conn.inbox.count == 0);
    }

    fn removeConnection(self: *Self, conn: *Connection) void {
        // Remove any subscribers/publishers this connection has
        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;

            const key = utils.generateKey(topic.topic_name, conn.connection_id);
            if (topic.subscribers.fetchRemove(key)) |subscriber_entry| {
                const subscriber = subscriber_entry.value;
                self.allocator.destroy(subscriber);
            }

            if (topic.publishers.fetchRemove(key)) |publisher_entry| {
                const publisher = publisher_entry.value;
                self.allocator.destroy(publisher);
            }

            // deinit the topic if there are no publishers or subscribers left
            if (topic.subscribers.count() == 0 and topic.publishers.count() == 0) {
                const topic_name = topic.topic_name;
                topic.deinit();
                self.allocator.destroy(topic);
                assert(self.topics.remove(topic_name));
            }
        }

        _ = self.connections.remove(conn.connection_id);

        log.info("node: {} removed connection {}", .{ self.id, conn.connection_id });
        conn.deinit();
        self.allocator.destroy(conn);
    }
};

pub const ConnectionHandle = struct {
    const Self = @This();

    connection: *Connection,
    worker: *Worker,

    pub fn enqueueMessage(self: Self, message: *Message) !void {
        _ = self;
        _ = message;

        // self.worker.outbox_mutex.lock();
        // defer self.worker.outbox_mutex.unlock();

        // try self.worker.outbox.enqueue(message);
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

    try node.start();
    defer node.close();
}
