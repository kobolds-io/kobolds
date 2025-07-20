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
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Connection = @import("../protocol/connection.zig").Connection;

const Publisher = @import("../pubsub/publisher.zig").Publisher;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const Topic = @import("../pubsub/topic.zig").Topic;
const TopicOptions = @import("../pubsub/topic.zig").TopicOptions;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;
const Envelope = @import("../data_structures/envelope.zig").Envelope;

pub const PingOptions = struct {};

pub const NodeConfig = struct {
    const Self = @This();

    worker_threads: usize = 3,
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 500_000,
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
    topics: std.StringHashMap(*Topic),
    topics_mutex: std.Thread.Mutex,
    subscribers: std.AutoHashMap(u128, *Subscriber),
    metrics: Metrics,
    inbox: *RingBuffer(*Message),
    connection_outboxes: std.AutoHashMap(u128, *RingBuffer(Envelope)),
    connection_outboxes_mutex: std.Thread.Mutex,

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

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, config.memory_pool_capacity);
        errdefer inbox.deinit();

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
            .topics = std.StringHashMap(*Topic).init(allocator),
            .topics_mutex = std.Thread.Mutex{},
            .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .mutex = std.Thread.Mutex{},
            .metrics = .{},
            .inbox = inbox,
            .connection_outboxes = std.AutoHashMap(u128, *RingBuffer(Envelope)).init(allocator),
            .connection_outboxes_mutex = std.Thread.Mutex{},
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

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;

            while (subscriber.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            subscriber.deinit();
            self.allocator.destroy(subscriber);
        }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        var connection_outboxes_iter = self.connection_outboxes.valueIterator();
        while (connection_outboxes_iter.next()) |entry| {
            const outbox = entry.*;

            while (outbox.dequeue()) |envelope| {
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            outbox.deinit();
            self.allocator.destroy(outbox);
        }

        self.listeners.deinit();
        self.workers.deinit();
        self.remotes.deinit();
        self.memory_pool.deinit();
        self.io.deinit();
        self.connections.deinit();
        self.topics.deinit();
        self.inbox.deinit();
        self.connection_outboxes.deinit();
        self.subscribers.deinit();

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.connections);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.remotes);
        self.allocator.destroy(self.workers);
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

                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                    // self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
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

                // spin down the listeners
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
        const now_ms = std.time.milliTimestamp();
        const difference = now_ms - self.metrics.last_printed_at_ms;
        if (difference >= 1_000) {
            const messages_processed = self.metrics.messages_processed.load(.seq_cst);
            const delta = messages_processed - self.metrics.last_messages_processed_printed;
            self.metrics.last_messages_processed_printed = messages_processed;
            self.metrics.last_printed_at_ms = std.time.milliTimestamp();
            log.info("time since last print {}ms, memory_pool.available: {}, messages processed {}, delta {}", .{
                difference,
                self.memory_pool.available(),
                self.metrics.last_messages_processed_printed,
                delta,
            });
        }

        try self.pruneDeadConnections();
        try self.maybeAddInboundConnections();
        try self.gatherMessages();
        try self.processMessages();
        try self.aggregateMessages();
        try self.distributeMessages();
    }

    fn gatherMessages(self: *Self) !void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_entry| {
            if (self.inbox.available() == 0) break;

            const worker = worker_entry.*;

            worker.inbox_mutex.lock();
            defer worker.inbox_mutex.unlock();

            if (worker.inbox.isEmpty()) continue;

            self.inbox.concatenateAvailable(worker.inbox);
        }
    }

    fn processMessages(self: *Self) !void {
        if (self.inbox.count == 0) return;

        // There should only be `n` messages processed every tick
        const max_messages_processed_per_tick: usize = 100_000;
        var i: usize = 0;
        while (i < max_messages_processed_per_tick) : (i += 1) {
            while (self.inbox.dequeue()) |message| {
                assert(message.refs() == 1);
                defer {
                    _ = self.metrics.messages_processed.fetchAdd(1, .seq_cst);
                    message.deref();
                    if (message.refs() == 0) self.memory_pool.destroy(message);
                }

                switch (message.headers.message_type) {
                    .publish => try self.handlePublish(message),
                    .subscribe => try self.handleSubscribe(message),
                    else => |t| {
                        log.err("received unhandled message type {any}", .{t});
                        @panic("unhandled message!");
                    },
                }
            }

            var topics_iter = self.topics.valueIterator();
            while (topics_iter.next()) |topic_entry| {
                const topic = topic_entry.*;
                try topic.tick();
            }
        }
    }

    fn aggregateMessages(self: *Self) !void {
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |subscriber_entry| {
            const subscriber: *Subscriber = subscriber_entry.*;
            const connection_outbox = try self.findOrCreateConnectionOutbox(subscriber.conn_id);

            // We are going to create envelopes here
            while (connection_outbox.available() > 0 and !subscriber.queue.isEmpty()) {
                if (subscriber.queue.dequeue()) |message| {
                    const envelope = Envelope{
                        .connection_id = subscriber.conn_id,
                        .message = message,
                    };

                    // we are checking this loop that adding the envelope will be successful
                    connection_outbox.enqueue(envelope) catch unreachable;
                }
            }
        }
    }

    fn distributeMessages(self: *Self) !void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_entry| {
            const worker = worker_entry.*;

            worker.outbox_mutex.lock();
            defer worker.outbox_mutex.unlock();

            if (worker.outbox.isFull()) continue;

            worker.connections_mutex.lock();
            defer worker.connections_mutex.unlock();

            // FIX: this is pretty inefficient as we are re looking up the connections for each worker every single
            // time. A better would be to already know the worker the messages were destined for before we even get to
            // this loop.
            var worker_connections_iter = worker.connections.keyIterator();
            while (worker_connections_iter.next()) |connection_id_entry| {
                const conn_id: uuid.Uuid = connection_id_entry.*;

                if (self.connection_outboxes.get(conn_id)) |connection_outbox| {
                    worker.outbox.concatenateAvailable(connection_outbox);
                } else {
                    // this connection doesn't have an outbox.
                    const connection_outbox = try self.findOrCreateConnectionOutbox(conn_id);
                    worker.outbox.concatenateAvailable(connection_outbox);
                }
            }
        }
    }

    fn pruneDeadConnections(self: *Self) !void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_entry| {
            const worker = worker_entry.*;

            worker.dead_connections_mutex.lock();
            defer worker.dead_connections_mutex.unlock();

            for (worker.dead_connections.items) |conn_id| {
                // Loop over all connection_outboxes
                var topics_iter = self.topics.valueIterator();
                while (topics_iter.next()) |entry| {
                    const topic = entry.*;

                    const key = utils.generateKey(topic.topic_name, conn_id);
                    if (topic.subscribers.fetchRemove(key)) |subscriber_entry| {
                        const subscriber = subscriber_entry.value;
                        while (subscriber.queue.dequeue()) |message| {
                            message.deref();
                            if (message.refs() == 0) self.memory_pool.destroy(message);
                        }

                        self.allocator.destroy(subscriber);
                    }

                    // if (topic.publishers.fetchRemove(key)) |publisher_entry| {
                    //     const publisher = publisher_entry.value;
                    //     self.allocator.destroy(publisher);
                    // }

                    // FIX: we should have publishers as well. Publishers can timeout and that will help
                    // us determine if we should destroy the topic or not
                    // // deinit the topic if there are no publishers or subscribers left
                    // if (topic.subscribers.count() == 0) {
                    //     const topic_name = topic.topic_name;
                    //     topic.deinit();
                    //     self.allocator.destroy(topic);
                    //     assert(self.topics.remove(topic_name));
                    // }
                }

                if (self.connection_outboxes.get(conn_id)) |outbox| {
                    while (outbox.dequeue()) |envelope| {
                        const message = envelope.message;
                        message.deref();
                        if (message.refs() == 0) self.memory_pool.destroy(message);
                    }
                }

                log.info("node: {} removed connection {}", .{ self.id, conn_id });
            }

            // remove all the dead connections from the list
            worker.dead_connections.items.len = 0;
        }
    }

    fn findOrCreateConnectionOutbox(self: *Self, conn_id: u128) !*RingBuffer(Envelope) {
        var connection_outbox: *RingBuffer(Envelope) = undefined;
        if (self.connection_outboxes.get(conn_id)) |queue| {
            connection_outbox = queue;
        } else {
            const queue = try self.allocator.create(RingBuffer(Envelope));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(Envelope).init(self.allocator, constants.connection_outbox_capacity);
            errdefer queue.deinit();

            try self.connection_outboxes.put(conn_id, queue);
            connection_outbox = queue;
        }

        return connection_outbox;
    }

    fn initializeWorkers(self: *Self) !void {
        assert(self.workers.count() == 0);

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

                while (listener.sockets.pop()) |socket| {
                    try self.addInboundConnectionToNextWorker(socket);
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
        assert(self.workers.count() > 0);
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // find the worker with the least number of connections
        var worker_iter = self.workers.valueIterator();
        var worker_with_min_connections: ?*Worker = null;
        var min_connections: usize = 0;

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

    fn handlePublish(self: *Self, message: *Message) !void {
        // Publishes actually don't care about the origin of the message so much. Instead, they care much more about
        // the destination of the mssage. The topic is in charge of distributing messages to subscribers. Subscribers
        // are in charge of attaching metadata as to the destination of the message
        const topic = try self.findOrCreateTopic(message.topicName(), .{});
        if (topic.queue.available() == 0) {
            try topic.tick();
        }
        message.ref();
        errdefer message.deref();
        try topic.queue.enqueue(message);
    }

    fn handleSubscribe(self: *Self, message: *Message) !void {
        // FIX: we should ensure that the outbox has sufficient space before even attempting to process this

        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new();
        reply.headers.message_type = .reply;
        reply.setTopicName(message.topicName());
        reply.setTransactionId(message.transactionId());
        reply.setErrorCode(.ok);
        reply.ref();

        const connection_outbox = try self.findOrCreateConnectionOutbox(message.headers.connection_id);
        if (connection_outbox.isFull()) {
            return error.ConnectionOutboxFull;
        }

        // QUESTION: A connection can only be subscribed to a topic ONCE. Is this good behavior???
        const subscriber_key = utils.generateKey(message.topicName(), message.headers.connection_id);
        if (self.subscribers.get(subscriber_key)) |_| {
            // FIX: pick a better error for this
            reply.setErrorCode(.err);
            log.err("duplicate connection subscription", .{});

            const envelope = Envelope{
                .connection_id = message.headers.connection_id,
                .message = reply,
            };

            try connection_outbox.enqueue(envelope);
            return;
        }

        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = try Subscriber.init(
            self.allocator,
            subscriber_key,
            message.headers.connection_id,
            constants.subscriber_max_queue_capacity,
        );
        errdefer subscriber.deinit();

        try self.subscribers.put(subscriber_key, subscriber);
        errdefer _ = self.subscribers.remove(subscriber_key);

        try self.addSubscriber(message.topicName(), subscriber);
        errdefer self.removeSubscriber(message.topicName(), subscriber_key);

        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = reply,
        };

        try connection_outbox.enqueue(envelope);
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

    pub fn findOrCreateTopic(self: *Self, topic_name: []const u8, options: TopicOptions) !*Topic {
        _ = options;
        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        if (self.topics.get(topic_name)) |t| {
            return t;
        } else {
            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);

            topic.* = try Topic.init(self.allocator, self.memory_pool, topic_name);
            errdefer topic.deinit();

            try self.topics.put(topic_name, topic);
            return topic;
        }
    }

    pub fn addSubscriber(self: *Self, topic_name: []const u8, subscriber: *Subscriber) !void {
        const topic = try self.findOrCreateTopic(topic_name, .{});

        try topic.subscribers.put(subscriber.key, subscriber);
    }

    pub fn removeSubscriber(self: *Self, topic_name: []const u8, subscriber_key: u128) void {
        const topic = self.findOrCreateTopic(topic_name, .{}) catch |err| {
            log.err("could not find or create topic {any}", .{err});
            return;
        };

        _ = topic.subscribers.remove(subscriber_key);
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
};

pub const ConnectionHandle = struct {
    const Self = @This();

    connection: *Connection,
    worker: *Worker,

    pub fn enqueueMessage(self: Self, message: *Message) !void {
        _ = self;
        _ = message;
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
