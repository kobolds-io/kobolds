const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const KID = @import("kid").KID;

const IO = @import("../io.zig").IO;
const Worker = @import("./worker.zig").Worker;
const Listener = @import("./listener.zig").Listener;
const ListenerConfig = @import("./listener.zig").ListenerConfig;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const Connection = @import("../protocol/connection.zig").Connection;
const PeerType = @import("../protocol/connection.zig").PeerType;
const NodeMetrics = @import("./metrics.zig").NodeMetrics;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;

const Publisher = @import("../pubsub/publisher.zig").Publisher;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const Topic = @import("../pubsub/topic.zig").Topic;
const TopicOptions = @import("../pubsub/topic.zig").TopicOptions;

const LoadBalancer = @import("../load_balancers/load_balancer.zig").LoadBalancer;

const Service = @import("../services/service.zig").Service;
const ServiceOptions = @import("../services/service.zig").ServiceOptions;
const Advertiser = @import("../services/advertiser.zig").Advertiser;
const Requestor = @import("../services/requestor.zig").Requestor;

const Envelope = @import("./envelope.zig").Envelope;

const Session = @import("./session.zig").Session;

const Authenticator = @import("./authenticator.zig").Authenticator;
const AuthenticatorConfig = @import("./authenticator.zig").AuthenticatorConfig;

pub const NodeConfig = struct {
    const Self = @This();

    node_id: u11 = 0,
    worker_threads: usize = 3,
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = constants.default_node_memory_pool_capacity,
    listener_configs: ?[]const ListenerConfig = null,
    outbound_configs: ?[]const OutboundConnectionConfig = null,
    authenticator_config: AuthenticatorConfig = .{ .none = .{} },

    pub fn validate(self: Self) ?[]const u8 {
        const cpu_core_count = std.Thread.getCpuCount() catch @panic("could not get getCpuCount");

        if (self.worker_threads > cpu_core_count) return "NodeConfig `worker_threads` exceeds cpu core count";
        if (self.max_connections > 1024) return "NodeConfig `max_connections` exceeds arbitrary limit";
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

        // if (self.authenticator_config.validate()) |reason| return reason;

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

    allocator: std.mem.Allocator,
    authenticator: *Authenticator,
    close_channel: *UnbufferedChannel(bool),
    config: NodeConfig,
    session_outboxes: std.AutoHashMapUnmanaged(u64, *RingBuffer(Envelope)),
    connection_outboxes: std.AutoHashMapUnmanaged(u64, *RingBuffer(Envelope)),
    done_channel: *UnbufferedChannel(bool),
    inbox: *RingBuffer(Envelope),
    io: *IO,
    kid: KID,
    listeners: *std.AutoHashMap(usize, *Listener),
    memory_pool: *MemoryPool(Message),
    metrics: NodeMetrics,
    mutex: std.Thread.Mutex,
    services: std.StringHashMap(*Service),
    state: NodeState,
    topics: std.StringHashMapUnmanaged(*Topic),
    workers: *std.AutoHashMap(usize, *Worker),
    workers_load_balancer: LoadBalancer(usize),
    sessions: std.AutoHashMapUnmanaged(u64, *Session),
    sessions_mutex: std.Thread.Mutex,

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

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        const inbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(Envelope).init(allocator, config.memory_pool_capacity);
        errdefer inbox.deinit();

        const listeners = try allocator.create(std.AutoHashMap(usize, *Listener));
        errdefer allocator.destroy(listeners);

        listeners.* = std.AutoHashMap(usize, *Listener).init(allocator);
        errdefer listeners.deinit();

        const authenticator = try allocator.create(Authenticator);
        errdefer allocator.destroy(authenticator);

        authenticator.* = try Authenticator.init(allocator, config.authenticator_config);
        errdefer authenticator.deinit();

        return Self{
            .allocator = allocator,
            .authenticator = authenticator,
            .close_channel = close_channel,
            .config = config,
            .session_outboxes = .empty,
            .connection_outboxes = .empty,
            .done_channel = done_channel,
            .inbox = inbox,
            .io = io,
            .kid = KID.init(config.node_id, .{}),
            .listeners = listeners,
            .memory_pool = memory_pool,
            .metrics = .{},
            .mutex = std.Thread.Mutex{},
            .services = std.StringHashMap(*Service).init(allocator),
            .state = .closed,
            .topics = .empty,
            .workers = workers,
            .sessions = .empty,
            .sessions_mutex = std.Thread.Mutex{},
            .workers_load_balancer = LoadBalancer(usize){
                .round_robin = .init(),
            },
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

        var topics_iter = self.topics.iterator();
        while (topics_iter.next()) |entry| {
            const topic_name = entry.key_ptr.*;
            const topic = entry.value_ptr.*;
            topic.deinit();
            self.allocator.destroy(topic);
            self.allocator.free(topic_name);
        }

        var services_iter = self.services.valueIterator();
        while (services_iter.next()) |entry| {
            const service = entry.*;
            service.deinit();
            self.allocator.destroy(service);
        }

        var sessions_iter = self.sessions.valueIterator();
        while (sessions_iter.next()) |entry| {
            const session = entry.*;
            session.deinit(self.allocator);
            self.allocator.destroy(session);
        }

        while (self.inbox.dequeue()) |envelope| {
            envelope.message.deref();
            if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
        }

        var connection_outboxes_iter = self.session_outboxes.valueIterator();
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
        self.memory_pool.deinit();
        self.io.deinit();
        self.topics.deinit(self.allocator);
        self.services.deinit();
        self.inbox.deinit();
        self.session_outboxes.deinit(self.allocator);
        self.connection_outboxes.deinit(self.allocator);
        self.authenticator.deinit();
        self.sessions.deinit(self.allocator);
        switch (self.workers_load_balancer) {
            .round_robin => |*lb| lb.deinit(self.allocator),
        }

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.workers);
        self.allocator.destroy(self.authenticator);
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
        // log.info("node {d} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                // log.info("node {d} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;

                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                    // self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
                },
                .closing => {
                    // log.info("node {d}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    pub fn close(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        switch (self.state) {
            .closed, .closing => return,
            else => {

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
        self.handlePrintingIntervalMetrics();

        self.pruneEmptySessions();
        // try self.pruneDeadConnections();
        try self.maybeAddInboundConnections();
        try self.gatherMessages();
        try self.processMessages();
        try self.aggregateMessages();
        try self.distributeMessages();
    }

    fn handlePrintingIntervalMetrics(self: *Self) void {
        const now_ms = std.time.milliTimestamp();
        const difference = now_ms - self.metrics.last_printed_at_ms;
        if (difference >= 1_000) {
            self.metrics.last_printed_at_ms = std.time.milliTimestamp();

            const messages_processed = self.metrics.messages_processed.load(.seq_cst);
            const messages_processed_delta = messages_processed - self.metrics.last_messages_processed_printed;
            self.metrics.last_messages_processed_printed = messages_processed;

            const bytes_processed = self.metrics.bytes_processed;
            const bytes_processed_delta = bytes_processed - self.metrics.last_bytes_processed_printed;
            self.metrics.last_bytes_processed_printed = bytes_processed;

            log.info("messages_processed: {d}, bytes_processed: {d}, messages_delta: {d}, bytes_delta: {d}, memory_pool: {d}", .{
                messages_processed,
                bytes_processed,
                messages_processed_delta,
                bytes_processed_delta,
                self.memory_pool.available(),
            });
        }
    }

    fn gatherMessages(self: *Self) !void {
        switch (self.workers_load_balancer) {
            .round_robin => |*lb| {
                var iters: usize = 0;

                while (iters < self.workers.count()) : (iters += 1) {
                    if (self.inbox.available() == 0) return;

                    if (lb.next()) |worker_id| {
                        if (self.workers.get(worker_id)) |worker| {
                            worker.inbox_mutex.lock();
                            defer worker.inbox_mutex.unlock();

                            if (worker.inbox.isEmpty()) continue;

                            self.inbox.concatenateAvailable(worker.inbox);
                        } else @panic("failed to get worker");
                    }
                }
            },
        }
    }

    fn processMessages(self: *Self) !void {
        while (self.inbox.dequeue()) |envelope| {
            assert(envelope.message.refs() == 1);
            defer {
                _ = self.metrics.messages_processed.fetchAdd(1, .seq_cst);
                self.metrics.bytes_processed += @intCast(envelope.message.packedSize());
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            // ensure that the message
            switch (envelope.message.fixed_headers.message_type) {
                .publish => try self.handlePublish(envelope),
                .subscribe => try self.handleSubscribe(envelope),
                .unsubscribe => try self.handleUnsubscribe(envelope),
                else => {},
            }
        }

        var topics_iter = self.topics.iterator();
        while (topics_iter.next()) |entry| {
            const topic_name = entry.key_ptr.*;
            _ = topic_name;
            const topic = entry.value_ptr.*;

            try topic.tick();
        }
    }

    fn aggregateMessages(self: *Self) !void {
        // aggregate all topic messages to the subscriber's session_outboxes
        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |topic_entry| {
            const topic: *Topic = topic_entry.*;
            if (topic.subscribers.count() == 0) continue;

            var subscribers_iter = topic.subscribers.valueIterator();
            while (subscribers_iter.next()) |subscriber_entry| {
                const subscriber: *Subscriber = subscriber_entry.*;

                const session_outbox = try self.findOrCreateSessionOutbox(subscriber.session_id);

                // we need to rewrite each envelope to use a different conn_id
                session_outbox.concatenateAvailable(subscriber.queue);
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

            // FIX: this should load balance accross connections. The issue with doing this is now I can't really
            // enforce any sort of order....is that ok? I think in the case where message rate is infrequent, this
            // really isn't an issue but in the case where we are sending hundreds or thousands of messages, this
            // becomes pain in the booty.
            var conn_sessions_iter = worker.conns_sessions.iterator();
            while (conn_sessions_iter.next()) |entry| {
                const conn_id = entry.key_ptr.*;
                const session_id = entry.value_ptr.*;

                if (self.session_outboxes.get(session_id)) |outbox| {
                    while (!outbox.isEmpty() and !worker.outbox.isFull()) {
                        const prev_envelope = outbox.dequeue().?;

                        const envelope = Envelope{
                            .message = prev_envelope.message,
                            .session_id = session_id,
                            .conn_id = conn_id,
                            .message_id = prev_envelope.message_id,
                        };

                        worker.outbox.enqueue(envelope) catch @panic("something modified this");
                    }
                }
            }
        }
    }

    fn pruneEmptySessions(self: *Self) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        var sessions_iter = self.sessions.iterator();
        while (sessions_iter.next()) |entry| {
            const session_id = entry.key_ptr.*;
            const session = entry.value_ptr.*;

            if (session.connections.count() == 0) {
                // Loop over all connection_outboxes
                var topics_iter = self.topics.valueIterator();
                while (topics_iter.next()) |topic_entry| {
                    const topic = topic_entry.*;

                    const subscriber_key = utils.generateKey64(topic.topic_name, session_id);
                    if (topic.subscribers.fetchRemove(subscriber_key)) |subscriber_entry| {
                        const subscriber = subscriber_entry.value;
                        while (subscriber.queue.dequeue()) |envelope| {
                            envelope.message.deref();
                            if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
                        }

                        log.debug("removing subscriber to {s} topic", .{topic.topic_name});

                        subscriber.deinit();
                        self.allocator.destroy(subscriber);
                    }
                }

                if (self.session_outboxes.fetchRemove(session_id)) |outbox_entry| {
                    const outbox = outbox_entry.value;
                    while (outbox.dequeue()) |envelope| {
                        envelope.message.deref();
                        if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
                    }

                    outbox.deinit();
                    self.allocator.destroy(outbox);
                }

                log.info("pruning empty session {}", .{session_id});
                session.deinit(self.allocator);
                self.allocator.destroy(session);

                assert(self.sessions.remove(session_id));
                log.info("successfully pruned session!", .{});
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

                        log.debug("removing subscriber to {s} topic", .{topic.topic_name});

                        subscriber.deinit();
                        self.allocator.destroy(subscriber);
                    }

                    // FIX: we should have topic publishers as well
                }

                var services_iter = self.services.valueIterator();
                while (services_iter.next()) |entry| {
                    const service = entry.*;

                    const advertiser_key = utils.generateKey(service.topic_name, conn_id);
                    _ = service.removeAdvertiser(advertiser_key);
                }

                if (self.session_outboxes.fetchRemove(conn_id)) |outbox_entry| {
                    const outbox = outbox_entry.value;
                    while (outbox.dequeue()) |envelope| {
                        const message = envelope.message;
                        message.deref();
                        if (message.refs() == 0) self.memory_pool.destroy(message);
                    }

                    outbox.deinit();
                    self.allocator.destroy(outbox);
                }

                // log.info("node: {d} removed connection {d}", .{ self.id, conn_id });
            }

            // remove all the dead connections from the list
            worker.dead_connections.items.len = 0;
        }
    }

    fn findOrCreateSessionOutbox(self: *Self, session_id: u64) !*RingBuffer(Envelope) {
        var session_outbox: *RingBuffer(Envelope) = undefined;
        if (self.session_outboxes.get(session_id)) |queue| {
            session_outbox = queue;
        } else {
            const queue = try self.allocator.create(RingBuffer(Envelope));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(Envelope).init(self.allocator, 1_000);
            errdefer queue.deinit();

            try self.session_outboxes.put(self.allocator, session_id, queue);
            session_outbox = queue;
        }

        return session_outbox;
    }

    fn findOrCreateConnectionOutbox(self: *Self, conn_id: u64) !*RingBuffer(Envelope) {
        var connection_outbox: *RingBuffer(Envelope) = undefined;
        if (self.connection_outboxes.get(conn_id)) |queue| {
            connection_outbox = queue;
        } else {
            const queue = try self.allocator.create(RingBuffer(Envelope));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(Envelope).init(self.allocator, 1_000);
            errdefer queue.deinit();

            try self.connection_outboxes.put(self.allocator, conn_id, queue);
            connection_outbox = queue;
        }

        return connection_outbox;
    }

    fn initializeWorkers(self: *Self) !void {
        assert(self.workers.count() == 0);

        for (0..self.config.worker_threads) |id| {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);

            worker.* = try Worker.init(self.allocator, self, id);
            errdefer worker.deinit();

            try self.workers.put(id, worker);
            errdefer _ = self.workers.remove(id);

            switch (self.workers_load_balancer) {
                .round_robin => |*lb| try lb.addItem(self.allocator, id),
            }
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
        _ = self;
        // if (self.config.outbound_configs) |outbound_configs| {
        //     for (outbound_configs) |outbound_config| {
        //         switch (outbound_config.transport) {
        //             .tcp => {
        //                 try self.addOutboundConnectionToNextWorker(outbound_config);
        //             },
        //         }
        //     }
        // }
    }

    fn maybeAddInboundConnections(self: *Self) !void {
        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;

            if (listener.sockets.items.len > 0) {
                listener.mutex.lock();
                defer listener.mutex.unlock();

                while (listener.sockets.pop()) |socket| {
                    try self.addInboundConnectionToNextWorker(socket, .{});
                }
            }
        }
    }

    fn addInboundConnectionToNextWorker(self: *Self, socket: posix.socket_t, config: InboundConnectionConfig) !void {
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

        try worker.addInboundConnection(socket, config);
    }

    fn addOutboundConnectionToNextWorker(self: *Self, config: OutboundConnectionConfig) !void {
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

        try worker.addOutboundConnection(config);
    }

    fn handlePublish(self: *Self, envelope: Envelope) !void {
        assert(envelope.message.refs() == 1);

        const topic = try self.findOrCreateTopic(envelope.message.topicName(), .{});
        if (topic.queue.available() == 0) try topic.tick(); // if there is no space, try to advance the topic

        // log.info("node message.topicName(): {any}", .{envelope.message.topicName()});
        // log.info("node message.body(): {any}", .{envelope.message.body()});

        envelope.message.ref();
        topic.queue.enqueue(envelope) catch envelope.message.deref();
    }

    fn handleSubscribe(self: *Self, envelope: Envelope) !void {
        log.info("handleSubscribe: conn_id: {}, session_id: {}", .{ envelope.conn_id, envelope.session_id });
        assert(envelope.message.refs() == 1);

        const subscribe_ack = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(subscribe_ack);

        subscribe_ack.* = Message.new(.subscribe_ack);
        subscribe_ack.extension_headers.subscribe_ack.transaction_id =
            envelope.message.extension_headers.subscribe.transaction_id;
        subscribe_ack.extension_headers.subscribe_ack.error_code = .ok;
        subscribe_ack.ref();
        errdefer subscribe_ack.deref();

        const topic = try self.findOrCreateTopic(envelope.message.topicName(), .{});
        const subscriber_key = utils.generateKey64(envelope.message.topicName(), envelope.session_id);

        topic.addSubscriber(subscriber_key, envelope.session_id) catch |err| switch (err) {
            error.AlreadyExists => {},
            else => {
                subscribe_ack.extension_headers.subscribe_ack.error_code = .failure;
            },
        };

        const session_outbox = try self.findOrCreateSessionOutbox(envelope.session_id);

        const subscribe_ack_envelope = Envelope{
            .message = subscribe_ack,
            .message_id = self.kid.generate(),
            .session_id = envelope.session_id,
            .conn_id = envelope.conn_id,
        };

        try session_outbox.enqueue(subscribe_ack_envelope);
    }

    fn handleUnsubscribe(self: *Self, envelope: Envelope) !void {
        log.info("handleUnsubscribe: conn_id: {}, session_id: {}", .{ envelope.conn_id, envelope.session_id });
        assert(envelope.message.refs() == 1);

        const unsubscribe_ack = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(unsubscribe_ack);

        unsubscribe_ack.* = Message.new(.unsubscribe_ack);
        unsubscribe_ack.extension_headers.unsubscribe_ack.transaction_id =
            envelope.message.extension_headers.unsubscribe.transaction_id;
        unsubscribe_ack.extension_headers.unsubscribe_ack.error_code = .ok;
        unsubscribe_ack.ref();
        errdefer unsubscribe_ack.deref();

        const topic = try self.findOrCreateTopic(envelope.message.topicName(), .{});
        const subscriber_key = utils.generateKey64(envelope.message.topicName(), envelope.session_id);

        if (!topic.removeSubscriber(subscriber_key)) {
            unsubscribe_ack.extension_headers.unsubscribe_ack.error_code = .failure;
        }

        const session_outbox = try self.findOrCreateSessionOutbox(envelope.session_id);

        const unsubscribe_ack_envelope = Envelope{
            .message = unsubscribe_ack,
            .message_id = self.kid.generate(),
            .session_id = envelope.session_id,
            .conn_id = envelope.conn_id,
        };

        try session_outbox.enqueue(unsubscribe_ack_envelope);
    }

    // fn handleAdvertise(self: *Self, message: *Message) !void {
    //     const reply = try self.memory_pool.create();
    //     errdefer self.memory_pool.destroy(reply);

    //     reply.* = Message.new();
    //     reply.headers.message_type = .reply;
    //     reply.setTopicName(message.topicName());
    //     reply.setTransactionId(message.transactionId());
    //     reply.setErrorCode(.ok);
    //     reply.ref();
    //     errdefer reply.deref();

    //     const connection_outbox = try self.findOrCreateSessionOutbox(message.headers.connection_id);
    //     if (connection_outbox.isFull()) {
    //         return error.ConnectionOutboxFull;
    //     }

    //     const service = try self.findOrCreateService(message.topicName(), .{});

    //     // QUESTION: A connection can only be subscribed to a service ONCE. Is this good behavior???
    //     const advertiser_key = utils.generateKey(message.topicName(), message.headers.connection_id);

    //     service.addAdvertiser(advertiser_key, message.headers.connection_id) catch |err| {
    //         log.err("error adding advertiser: {any}", .{err});
    //         reply.setErrorCode(.err);
    //     };
    //     errdefer _ = service.removeAdvertiser(advertiser_key);

    //     const envelope = Envelope{
    //         .connection_id = message.headers.connection_id,
    //         .message = reply,
    //     };

    //     try connection_outbox.enqueue(envelope);
    // }

    // fn handleUnadvertise(self: *Self, message: *Message) !void {
    //     const reply = try self.memory_pool.create();
    //     errdefer self.memory_pool.destroy(reply);

    //     reply.* = Message.new();
    //     reply.headers.message_type = .reply;
    //     reply.setTopicName(message.topicName());
    //     reply.setTransactionId(message.transactionId());
    //     reply.setErrorCode(.ok);
    //     reply.ref();
    //     errdefer reply.deref();

    //     const connection_outbox = try self.findOrCreateSessionOutbox(message.headers.connection_id);
    //     if (connection_outbox.isFull()) {
    //         return error.ConnectionOutboxFull;
    //     }

    //     const service = try self.findOrCreateService(message.topicName(), .{});

    //     // QUESTION: A connection can only be subscribed to a service ONCE. Is this good behavior???
    //     const advertiser_key = utils.generateKey(message.topicName(), message.headers.connection_id);

    //     if (!service.removeAdvertiser(advertiser_key)) {
    //         // Advertiser did not exist on service
    //         reply.setErrorCode(.err);
    //     }

    //     const envelope = Envelope{
    //         .connection_id = message.headers.connection_id,
    //         .message = reply,
    //     };
    //     try connection_outbox.enqueue(envelope);
    // }

    // fn handleRequest(self: *Self, message: *Message) !void {
    //     assert(message.refs() == 1);
    //     const service = try self.findOrCreateService(message.topicName(), .{});

    //     if (service.requests_queue.available() == 0) {
    //         try service.tick();
    //     }

    //     if (service.requests_queue.available() > 0) {
    //         message.ref();
    //         try service.requests_queue.enqueue(message);
    //         return;
    //     }

    //     const reply = try self.memory_pool.create();
    //     errdefer self.memory_pool.destroy(reply);

    //     reply.* = Message.new();
    //     reply.headers.message_type = .reply;
    //     reply.setTopicName(message.topicName());
    //     reply.setTransactionId(message.transactionId());
    //     reply.setErrorCode(.err);
    //     reply.ref();
    //     errdefer reply.deref();

    //     const connection_outbox = try self.findOrCreateSessionOutbox(message.headers.connection_id);
    //     if (connection_outbox.isFull()) {
    //         return error.ConnectionOutboxFull;
    //     }

    //     const envelope = Envelope{
    //         .connection_id = message.headers.connection_id,
    //         .message = reply,
    //     };

    //     try connection_outbox.enqueue(envelope);
    // }

    // fn handleReply(self: *Self, message: *Message) !void {
    //     assert(message.refs() == 1);

    //     const service = try self.findOrCreateService(message.topicName(), .{});
    //     if (service.replies_queue.available() == 0) {
    //         try service.tick();
    //     }

    //     message.ref();
    //     service.replies_queue.enqueue(message) catch message.deref();
    // }

    // fn handlePing(self: *Self, message: *Message) !void {
    //     assert(message.refs() == 1);

    //     log.debug("received ping from origin_id: {d}, connection_id: {d}", .{
    //         message.headers.origin_id,
    //         message.headers.connection_id,
    //     });
    //     // Since this is a `ping` we don't need to do any extra work to figure out how to respond
    //     message.headers.message_type = .pong;
    //     // message.headers.origin_id = self.id;
    //     message.setTransactionId(message.transactionId());
    //     message.setErrorCode(.ok);

    //     const conn_outbox = self.findOrCreateSessionOutbox(message.headers.connection_id) catch |err| {
    //         log.err("Failed to findOrCreateConnectionOutbox: {any}", .{err});
    //         return;
    //     };

    //     message.ref();
    //     const envelope = Envelope{
    //         .connection_id = message.headers.connection_id,
    //         .message = message,
    //     };

    //     if (conn_outbox.enqueue(envelope)) |_| {} else |err| {
    //         log.err("Failed to enqueue message to conn_outbox: {any}", .{err});
    //         message.deref();
    //     }
    // }

    // fn handlePong(self: *Self, message: *Message) !void {
    //     defer {
    //         message.deref();
    //         if (message.refs() == 0) self.memory_pool.destroy(message);
    //     }

    //     log.debug("received pong from origin_id: {d}, connection_id: {d}", .{
    //         message.headers.origin_id,
    //         message.headers.connection_id,
    //     });
    // }

    fn findOrCreateTopic(self: *Self, topic_name: []const u8, options: TopicOptions) !*Topic {
        _ = options;

        if (self.topics.get(topic_name)) |t| {
            return t;
        } else {
            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);

            const t_name = try self.allocator.dupe(u8, topic_name);
            errdefer self.allocator.free(t_name);

            topic.* = try Topic.init(self.allocator, self.memory_pool, t_name, .{});
            errdefer topic.deinit();

            try self.topics.put(self.allocator, t_name, topic);
            return topic;
        }
    }

    fn findOrCreateService(self: *Self, topic_name: []const u8, options: ServiceOptions) !*Service {
        _ = options;

        if (self.services.get(topic_name)) |t| {
            return t;
        } else {
            const service = try self.allocator.create(Service);
            errdefer self.allocator.destroy(service);

            service.* = try Service.init(self.allocator, self.memory_pool, topic_name, .{});
            errdefer service.deinit();

            try self.services.put(topic_name, service);
            return service;
        }
    }

    pub fn createSession(self: *Self, peer_id: u64, peer_type: PeerType) !*Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        const session = try self.allocator.create(Session);
        errdefer self.allocator.destroy(session);

        const session_id = self.kid.generate();
        session.* = try Session.init(self.allocator, session_id, peer_id, peer_type, .round_robin);
        errdefer session.deinit(self.allocator);

        try self.sessions.put(self.allocator, session_id, session);

        return session;
    }

    pub fn removeSession(self: *Self, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            session.deinit(self.allocator);
            _ = self.sessions.remove(session_id);
        }
    }

    pub fn getSession(self: *Self, session_id: u64) ?*Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        return self.sessions.get(session_id);
    }

    pub fn addConnectionToSession(self: *Self, session_id: u64, conn: *Connection) !void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            try session.addConnection(self.allocator, conn);
        } else {
            return error.SessionNotFound;
        }
    }

    pub fn removeConnectionFromSession(self: *Self, session_id: u64, conn_id: u64) bool {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            return session.removeConnection(conn_id);
        }
        return false;
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
