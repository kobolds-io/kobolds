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
const Worker = @import("./worker2.zig").Worker;
const Listener = @import("./listener.zig").Listener;
const ListenerConfig = @import("./listener.zig").ListenerConfig;
const InboundConnectionConfig = @import("../protocol/connection2.zig").InboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;
const PeerType = @import("../protocol/connection2.zig").PeerType;
const NodeMetrics = @import("./metrics.zig").NodeMetrics;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message2.zig").Message;
const Connection = @import("../protocol/connection2.zig").Connection;

const Publisher = @import("../pubsub/publisher.zig").Publisher;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const Topic = @import("../pubsub/topic.zig").Topic;
const TopicOptions = @import("../pubsub/topic.zig").TopicOptions;

const Service = @import("../services/service.zig").Service;
const ServiceOptions = @import("../services/service.zig").ServiceOptions;
const Advertiser = @import("../services/advertiser.zig").Advertiser;
const Requestor = @import("../services/requestor.zig").Requestor;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;
// const Envelope = @import("../data_structures/envelope.zig").Envelope;
const Envelope = @import("./envelope.zig").Envelope;

const Session = @import("./session.zig").Session;

const Authenticator = @import("./authenticator.zig").Authenticator;
const AuthenticatorConfig = @import("./authenticator.zig").AuthenticatorConfig;

pub const NodeConfig = struct {
    const Self = @This();

    node_id: u11 = 0,
    worker_threads: usize = 3,
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 100_000,
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
    connection_outboxes: std.AutoHashMap(u128, *RingBuffer(Envelope)),
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
    topics: std.StringHashMap(*Topic),
    workers: *std.AutoHashMap(usize, *Worker),
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
            .connection_outboxes = std.AutoHashMap(u128, *RingBuffer(Envelope)).init(allocator),
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
            .topics = std.StringHashMap(*Topic).init(allocator),
            .workers = workers,
            .sessions = .empty,
            .sessions_mutex = std.Thread.Mutex{},
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

        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;
            topic.deinit();
            self.allocator.destroy(topic);
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
        self.memory_pool.deinit();
        self.io.deinit();
        self.topics.deinit();
        self.services.deinit();
        self.inbox.deinit();
        self.connection_outboxes.deinit();
        self.authenticator.deinit();
        self.sessions.deinit(self.allocator);

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
        _ = self;
        // // There should only be `n` messages processed every tick
        // const max_messages_processed_per_tick: usize = 50_000;
        // var i: usize = 0;
        // while (i < max_messages_processed_per_tick) : (i += 1) {
        //     if (self.inbox.dequeue()) |message| {
        //         assert(message.refs() == 1);
        //         defer {
        //             _ = self.metrics.messages_processed.fetchAdd(1, .seq_cst);
        //             self.metrics.bytes_processed += @intCast(message.size());
        //             message.deref();
        //             if (message.refs() == 0) self.memory_pool.destroy(message);
        //         }

        //         switch (message.headers.message_type) {
        //             .publish => try self.handlePublish(message),
        //             .subscribe => try self.handleSubscribe(message),
        //             .advertise => try self.handleAdvertise(message),
        //             .unadvertise => try self.handleUnadvertise(message),
        //             .request => try self.handleRequest(message),
        //             .reply => try self.handleReply(message),
        //             .ping => try self.handlePing(message),
        //             .pong => try self.handlePong(message),
        //             else => |t| {
        //                 log.err("received unhandled message type {any}", .{t});
        //                 @panic("unhandled message!");
        //             },
        //         }
        //     } else break;
        // }

        // var topics_iter = self.topics.valueIterator();
        // while (topics_iter.next()) |topic_entry| {
        //     const topic = topic_entry.*;
        //     try topic.tick();
        // }

        // var services_iter = self.services.valueIterator();
        // while (services_iter.next()) |service_entry| {
        //     const service = service_entry.*;
        //     try service.tick();
        // }
    }

    fn aggregateMessages(self: *Self) !void {
        _ = self;
        // var topics_iter = self.topics.valueIterator();
        // while (topics_iter.next()) |topic_entry| {
        //     const topic: *Topic = topic_entry.*;
        //     if (topic.subscribers.count() == 0) continue;

        //     var subscribers_iter = topic.subscribers.valueIterator();
        //     while (subscribers_iter.next()) |subscriber_entry| {
        //         const subscriber: *Subscriber = subscriber_entry.*;
        //         const connection_outbox = try self.findOrCreateConnectionOutbox(subscriber.conn_id);

        //         // We are going to create envelopes here
        //         while (connection_outbox.available() > 0 and !subscriber.queue.isEmpty()) {
        //             if (subscriber.queue.dequeue()) |message| {
        //                 const envelope = Envelope{
        //                     .connection_id = subscriber.conn_id,
        //                     .message = message,
        //                 };

        //                 // we are checking this loop that adding the envelope will be successful
        //                 connection_outbox.enqueue(envelope) catch unreachable;
        //             }
        //         }
        //     }
        // }

        // var services_iter = self.services.valueIterator();
        // while (services_iter.next()) |service_entry| {
        //     const service: *Service = service_entry.*;

        //     var advertisers_iter = service.advertisers.valueIterator();
        //     while (advertisers_iter.next()) |advertiser_entry| {
        //         const advertiser: *Advertiser = advertiser_entry.*;
        //         const connection_outbox = try self.findOrCreateConnectionOutbox(advertiser.conn_id);

        //         // We are going to create envelopes here
        //         while (connection_outbox.available() > 0 and !advertiser.queue.isEmpty()) {
        //             if (advertiser.queue.dequeue()) |message| {
        //                 const envelope = Envelope{
        //                     .connection_id = advertiser.conn_id,
        //                     .message = message,
        //                 };

        //                 // we are checking this loop that adding the envelope will be successful
        //                 connection_outbox.enqueue(envelope) catch unreachable;
        //             }
        //         }
        //     }

        //     var requestors_iter = service.requestors.valueIterator();
        //     while (requestors_iter.next()) |requestor_entry| {
        //         const requestor: *Requestor = requestor_entry.*;
        //         const connection_outbox = try self.findOrCreateConnectionOutbox(requestor.conn_id);

        //         // We are going to create envelopes here
        //         while (connection_outbox.available() > 0 and !requestor.queue.isEmpty()) {
        //             if (requestor.queue.dequeue()) |message| {
        //                 const envelope = Envelope{
        //                     .connection_id = requestor.conn_id,
        //                     .message = message,
        //                 };

        //                 // we are checking this loop that adding the envelope will be successful
        //                 connection_outbox.enqueue(envelope) catch unreachable;
        //             }
        //         }
        //     }
        // }
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

    fn pruneEmptySessions(self: *Self) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        var sessions_iter = self.sessions.iterator();
        while (sessions_iter.next()) |entry| {
            const session_id = entry.key_ptr.*;
            const session = entry.value_ptr.*;

            if (session.connections.count() == 0) {
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

                if (self.connection_outboxes.fetchRemove(conn_id)) |outbox_entry| {
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

            worker.* = try Worker.init(self.allocator, self, id);
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

    fn handlePublish(self: *Self, message: *Message) !void {
        assert(message.refs() == 1);
        // Publishes actually don't care about the origin of the message so much. Instead, they care much more about
        // the destination of the mssage. The topic is in charge of distributing messages to subscribers. Subscribers
        // are in charge of attaching metadata as to the destination of the message
        const topic = try self.findOrCreateTopic(message.topicName(), .{});
        if (topic.queue.available() == 0) {
            // Try and push messages to subscribers to free up slots in the topic
            try topic.tick();
        }

        message.ref();
        topic.queue.enqueue(message) catch message.deref();
    }

    fn handleSubscribe(self: *Self, message: *Message) !void {
        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new();
        reply.headers.message_type = .reply;
        reply.setTopicName(message.topicName());
        reply.setTransactionId(message.transactionId());
        reply.setErrorCode(.ok);
        reply.ref();
        errdefer reply.deref();

        const connection_outbox = try self.findOrCreateConnectionOutbox(message.headers.connection_id);
        if (connection_outbox.isFull()) {
            return error.ConnectionOutboxFull;
        }

        const topic = try self.findOrCreateTopic(message.topicName(), .{});

        // QUESTION: A connection can only be subscribed to a topic ONCE. Is this good behavior???
        const subscriber_key = utils.generateKey(message.topicName(), message.headers.connection_id);

        topic.addSubscriber(subscriber_key, message.headers.connection_id) catch |err| {
            log.err("error adding subscriber: {any}", .{err});
            reply.setErrorCode(.err);
        };
        errdefer _ = topic.removeSubscriber(subscriber_key);

        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = reply,
        };

        try connection_outbox.enqueue(envelope);
    }

    fn handleAdvertise(self: *Self, message: *Message) !void {
        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new();
        reply.headers.message_type = .reply;
        reply.setTopicName(message.topicName());
        reply.setTransactionId(message.transactionId());
        reply.setErrorCode(.ok);
        reply.ref();
        errdefer reply.deref();

        const connection_outbox = try self.findOrCreateConnectionOutbox(message.headers.connection_id);
        if (connection_outbox.isFull()) {
            return error.ConnectionOutboxFull;
        }

        const service = try self.findOrCreateService(message.topicName(), .{});

        // QUESTION: A connection can only be subscribed to a service ONCE. Is this good behavior???
        const advertiser_key = utils.generateKey(message.topicName(), message.headers.connection_id);

        service.addAdvertiser(advertiser_key, message.headers.connection_id) catch |err| {
            log.err("error adding advertiser: {any}", .{err});
            reply.setErrorCode(.err);
        };
        errdefer _ = service.removeAdvertiser(advertiser_key);

        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = reply,
        };

        try connection_outbox.enqueue(envelope);
    }

    fn handleUnadvertise(self: *Self, message: *Message) !void {
        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new();
        reply.headers.message_type = .reply;
        reply.setTopicName(message.topicName());
        reply.setTransactionId(message.transactionId());
        reply.setErrorCode(.ok);
        reply.ref();
        errdefer reply.deref();

        const connection_outbox = try self.findOrCreateConnectionOutbox(message.headers.connection_id);
        if (connection_outbox.isFull()) {
            return error.ConnectionOutboxFull;
        }

        const service = try self.findOrCreateService(message.topicName(), .{});

        // QUESTION: A connection can only be subscribed to a service ONCE. Is this good behavior???
        const advertiser_key = utils.generateKey(message.topicName(), message.headers.connection_id);

        if (!service.removeAdvertiser(advertiser_key)) {
            // Advertiser did not exist on service
            reply.setErrorCode(.err);
        }

        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = reply,
        };
        try connection_outbox.enqueue(envelope);
    }

    fn handleRequest(self: *Self, message: *Message) !void {
        assert(message.refs() == 1);
        const service = try self.findOrCreateService(message.topicName(), .{});

        if (service.requests_queue.available() == 0) {
            try service.tick();
        }

        if (service.requests_queue.available() > 0) {
            message.ref();
            try service.requests_queue.enqueue(message);
            return;
        }

        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new();
        reply.headers.message_type = .reply;
        reply.setTopicName(message.topicName());
        reply.setTransactionId(message.transactionId());
        reply.setErrorCode(.err);
        reply.ref();
        errdefer reply.deref();

        const connection_outbox = try self.findOrCreateConnectionOutbox(message.headers.connection_id);
        if (connection_outbox.isFull()) {
            return error.ConnectionOutboxFull;
        }

        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = reply,
        };

        try connection_outbox.enqueue(envelope);
    }

    fn handleReply(self: *Self, message: *Message) !void {
        assert(message.refs() == 1);

        const service = try self.findOrCreateService(message.topicName(), .{});
        if (service.replies_queue.available() == 0) {
            try service.tick();
        }

        message.ref();
        service.replies_queue.enqueue(message) catch message.deref();
    }

    fn handlePing(self: *Self, message: *Message) !void {
        assert(message.refs() == 1);

        log.debug("received ping from origin_id: {d}, connection_id: {d}", .{
            message.headers.origin_id,
            message.headers.connection_id,
        });
        // Since this is a `ping` we don't need to do any extra work to figure out how to respond
        message.headers.message_type = .pong;
        // message.headers.origin_id = self.id;
        message.setTransactionId(message.transactionId());
        message.setErrorCode(.ok);

        const conn_outbox = self.findOrCreateConnectionOutbox(message.headers.connection_id) catch |err| {
            log.err("Failed to findOrCreateConnectionOutbox: {any}", .{err});
            return;
        };

        message.ref();
        const envelope = Envelope{
            .connection_id = message.headers.connection_id,
            .message = message,
        };

        if (conn_outbox.enqueue(envelope)) |_| {} else |err| {
            log.err("Failed to enqueue message to conn_outbox: {any}", .{err});
            message.deref();
        }
    }

    fn handlePong(self: *Self, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        log.debug("received pong from origin_id: {d}, connection_id: {d}", .{
            message.headers.origin_id,
            message.headers.connection_id,
        });
    }

    fn findOrCreateTopic(self: *Self, topic_name: []const u8, options: TopicOptions) !*Topic {
        _ = options;

        if (self.topics.get(topic_name)) |t| {
            return t;
        } else {
            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);

            topic.* = try Topic.init(self.allocator, self.memory_pool, topic_name, .{});
            errdefer topic.deinit();

            try self.topics.put(topic_name, topic);
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
