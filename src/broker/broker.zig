const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.broker);
const posix = std.posix;

const constants = @import("../constants.zig");
const kid = @import("kid");
const utils = @import("../utils.zig");

const stdx = @import("stdx");
const MemoryPool = stdx.MemoryPool;
const RingBuffer = stdx.RingBuffer;
const UnbufferedChannel = stdx.UnbufferedChannel;
const IO = @import("../io.zig").IO;
const LoadBalancer = @import("../load_balancers/load_balancer.zig").LoadBalancer;
const Envelope = @import("./envelope.zig").Envelope;
const Session = @import("./session.zig").Session;

const Listener = @import("./listener.zig").Listener;
const ListenerConfig = @import("./listener.zig").ListenerConfig;

const Worker = @import("./worker.zig").Worker;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;
const PeerType = @import("../protocol/connection.zig").PeerType;
const ChallengeMethod = @import("../protocol/message.zig").ChallengeMethod;
const ChallengeAlgorithm = @import("../protocol/message.zig").ChallengeAlgorithm;

const Authenticator = @import("./authenticator.zig").Authenticator;
const AuthenticatorConfig = @import("./authenticator.zig").AuthenticatorConfig;
const TokenEntry = @import("./authenticator.zig").TokenAuthStrategy.TokenEntry;
const BrokerMetrics = @import("./metrics.zig").BrokerMetrics;

const Topic = @import("./topic.zig").Topic;
const TopicOptions = @import("./topic.zig").TopicOptions;
const Subscriber = @import("./subscriber.zig").Subscriber;

/// Broker is the central construct for interacting with Kobolds.
pub const Broker = struct {
    const Self = @This();

    pub const Config = struct {
        broker_id: u11 = 0,
        memory_pool_capacity: usize = constants.default_client_memory_pool_capacity,
        inbox_capacity: usize = constants.default_client_inbox_capacity,
        outbox_capacity: usize = constants.default_client_outbox_capacity,
        worker_threads: usize = 1,
        listener_configs: ?[]const ListenerConfig = null,
        authenticator_config: AuthenticatorConfig = .{ .none = .{} },
    };

    const State = enum {
        running,
        closing,
        closed,
    };

    const Handshake = struct {
        nonce: u128,
        connection_id: u64,
        challenge_method: ChallengeMethod,
        algorithm: ChallengeAlgorithm,
    };

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    config: Config,
    connection_outboxes: std.AutoHashMapUnmanaged(u64, *RingBuffer(Envelope)),
    done_channel: *UnbufferedChannel(bool),
    handshakes: std.AutoHashMapUnmanaged(u64, Handshake),
    id: u11,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(Envelope),
    io: *IO,
    listeners: *std.AutoHashMapUnmanaged(usize, *Listener),
    memory_pool: *MemoryPool(Message),
    metrics: BrokerMetrics,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(Envelope),
    session_outboxes: std.AutoHashMapUnmanaged(u64, *RingBuffer(Envelope)),
    sessions_mutex: std.Thread.Mutex,
    sessions: std.AutoHashMapUnmanaged(u64, *Session),
    state: State,
    topics: std.StringHashMapUnmanaged(*Topic),
    workers_load_balancer: LoadBalancer(usize),
    workers: *std.AutoHashMapUnmanaged(usize, *Worker),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        const inbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(Envelope).init(allocator, config.inbox_capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(Envelope).init(allocator, config.outbox_capacity);
        errdefer outbox.deinit();

        const workers = try allocator.create(std.AutoHashMapUnmanaged(usize, *Worker));
        errdefer allocator.destroy(workers);

        workers.* = .empty;
        errdefer workers.deinit(allocator);

        const listeners = try allocator.create(std.AutoHashMapUnmanaged(usize, *Listener));
        errdefer allocator.destroy(listeners);

        listeners.* = .empty;
        errdefer listeners.deinit(allocator);

        kid.configure(config.broker_id, .{});

        return Self{
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            .connection_outboxes = .empty,
            .done_channel = done_channel,
            .handshakes = .empty,
            .id = config.broker_id,
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .io = io,
            .listeners = listeners,
            .memory_pool = memory_pool,
            .metrics = .{},
            .outbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .session_outboxes = .empty,
            .sessions = .empty,
            .sessions_mutex = std.Thread.Mutex{},
            .state = .closed,
            .topics = .empty,
            .workers = workers,
            .workers_load_balancer = LoadBalancer(usize){
                .round_robin = .init(),
            },
        };
    }

    pub fn deinit(self: *Self) void {
        var topics_iterator = self.topics.valueIterator();
        while (topics_iterator.next()) |entry| {
            const topic = entry.*;

            self.allocator.free(topic.topic_name);

            topic.deinit();
            self.allocator.destroy(topic);
        }

        var workers_iterator = self.workers.valueIterator();
        while (workers_iterator.next()) |entry| {
            const worker = entry.*;
            worker.deinit();
            self.allocator.destroy(worker);
        }

        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;
            listener.deinit();
            self.allocator.destroy(listener);
        }

        while (self.inbox.dequeue()) |envelope| {
            const message = envelope.message;
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        while (self.outbox.dequeue()) |envelope| {
            const message = envelope.message;
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        var sessions_iter = self.sessions.valueIterator();
        while (sessions_iter.next()) |entry| {
            const session = entry.*;
            session.deinit(self.allocator);
            self.allocator.destroy(session);
        }

        var session_outboxes_iter = self.session_outboxes.valueIterator();
        while (session_outboxes_iter.next()) |entry| {
            const outbox = entry.*;

            while (outbox.dequeue()) |envelope| {
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            outbox.deinit();
            self.allocator.destroy(outbox);
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
        // self.connections.deinit();
        // self.transactions.deinit(self.allocator);
        // self.topics.deinit();
        self.connection_outboxes.deinit(self.allocator);
        self.handshakes.deinit(self.allocator);
        self.inbox.deinit();
        self.io.deinit();
        self.listeners.deinit(self.allocator);
        self.memory_pool.deinit();
        self.outbox.deinit();
        self.session_outboxes.deinit(self.allocator);
        self.sessions.deinit(self.allocator);
        self.topics.deinit(self.allocator);
        self.workers.deinit(self.allocator);
        switch (self.workers_load_balancer) {
            .round_robin => |*lb| lb.deinit(self.allocator),
        }

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.outbox);
        self.allocator.destroy(self.workers);
    }

    pub fn start(self: *Self) !void {
        assert(self.state == .closed);

        // Start the workers
        try self.initializeWorkers();
        try self.spawnWorkerThreads();

        // Start the listeners
        try self.initializeListeners();
        // try self.spawnListeners();

        // // Start the outbound connections
        // try self.initializeOutboundConnections();

        // Start the core thread
        var ready_channel = UnbufferedChannel(bool).new();
        const core_thread = try std.Thread.spawn(.{}, Broker.run, .{ self, &ready_channel });
        core_thread.detach();

        _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("core_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                var all_workers_closed = true;
                const safety_limit: usize = 3;
                var i: usize = 0;
                while (i < safety_limit) : (i += 1) {
                    // spin down the workers
                    var worker_iterator = self.workers.valueIterator();
                    while (worker_iterator.next()) |entry| {
                        const worker = entry.*;
                        if (worker.state == .closed) continue;

                        worker.close();
                        all_workers_closed = false;
                    }

                    if (all_workers_closed) break;

                    // reset
                    all_workers_closed = true;
                } else @panic("safety limit breached");

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
        log.info("broker {d}: closed", .{self.id});
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        self.state = .running;
        ready_channel.send(true);
        log.info("broker {d} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("broker {d} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch |err| {
                        log.err("tick failed! {any}", .{err});
                        @panic("core tick failed");
                    };

                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                },
                .closing => {
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    fn tick(self: *Self) !void {
        self.handlePrintingIntervalMetrics();
        try self.gatherInboundSockets();
        try self.gatherEnvelopes();
        try self.processInboundEnvelopes();
        try self.tickTopics();
        try self.aggregateMessages();
        try self.distributeEnvelopes();
    }

    fn gatherEnvelopes(self: *Self) !void {
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

                            // const gathered_count = @min(worker.inbox.count, self.inbox.available());
                            // log.info("node gathered {}/msgs from worker {}", .{
                            //     gathered_count,
                            //     worker.id,
                            // });

                            _ = self.inbox.concatenateAvailable(worker.inbox);
                        } else @panic("failed to get worker");
                    }
                }
            },
        }
    }

    fn gatherInboundSockets(self: *Self) !void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |entry| {
            const worker = entry.*;

            worker.inbound_sockets_mutex.lock();
            defer worker.inbound_sockets_mutex.unlock();

            if (worker.inbound_sockets.items.len > 0) {
                while (worker.inbound_sockets.pop()) |socket| {
                    try self.addInboundConnectionToNextWorker(socket);
                }
            }
        }
    }

    fn processInboundEnvelopes(self: *Self) !void {
        var processed_messages_count: i64 = 0;
        var processed_bytes_count: i64 = 0;

        defer {
            _ = self.metrics.messages_processed.fetchAdd(processed_messages_count, .seq_cst);
            self.metrics.bytes_processed += processed_bytes_count;
        }

        while (self.inbox.dequeue()) |envelope| {
            assert(envelope.message.refs() == 1);

            defer {
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            processed_messages_count += 1;
            processed_bytes_count += @intCast(envelope.message.packedSize());

            // ensure that the message
            switch (envelope.message.fixed_headers.message_type) {
                .session_init => try self.handleSessionInit(envelope),
                .session_join => try self.handleSessionJoin(envelope),
                .publish => try self.handlePublish(envelope),
                .subscribe => try self.handleSubscribe(envelope),
                // .unsubscribe => try self.handleUnsubscribe(envelope),
                else => |t| {
                    log.info("got t: {any}", .{t});
                },
            }
        }
    }

    fn tickTopics(self: *Self) !void {
        // aggregate all topic messages to the subscriber's session_outboxes
        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |topic_entry| {
            const topic: *Topic = topic_entry.*;

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
                if (subscriber.queue.isEmpty()) continue;

                const session_outbox = try self.findOrCreateSessionOutbox(subscriber.session_id);

                // we need to rewrite each envelope to use a different conn_id
                _ = session_outbox.concatenateAvailable(subscriber.queue);
            }
        }
    }

    // FIX: this should load balance accross connections. The issue with doing this is now I can't really
    // enforce any sort of order....is that ok? I think in the case where message rate is infrequent, this
    // really isn't an issue but in the case where we are sending hundreds or thousands of messages, this
    // becomes pain in the booty.
    fn distributeEnvelopes(self: *Self) !void {
        var workers_iter = self.workers.valueIterator();
        while (workers_iter.next()) |worker_entry| {
            const worker = worker_entry.*;

            worker.outbox_mutex.lock();
            defer worker.outbox_mutex.unlock();

            if (worker.outbox.isFull()) continue;

            worker.conns_sessions_mutex.lock();
            defer worker.conns_sessions_mutex.unlock();

            var conn_sessions_iter = worker.conns_sessions.iterator();
            while (conn_sessions_iter.next()) |entry| {
                const conn_id = entry.key_ptr.*;
                const session_id = entry.value_ptr.*;

                if (self.session_outboxes.get(session_id)) |outbox| {
                    while (!outbox.isEmpty() and !worker.outbox.isFull()) {
                        const prev_envelope = outbox.dequeue().?;

                        // switch (prev_envelope.message.fixed_headers.message_type) {
                        //     .auth_success, .auth_failure => {
                        //         // we need to know if this connection is a part of this worker
                        //     },
                        //     else => {},
                        // }

                        const envelope = Envelope{
                            .message = prev_envelope.message,
                            .session_id = session_id,
                            .conn_id = conn_id,
                            .message_id = prev_envelope.message_id,
                        };

                        log.debug("sending envelope.message to session_outbox: {any}", .{envelope.message.fixed_headers});

                        worker.outbox.enqueue(envelope) catch @panic("something modified this");
                    }
                }

                if (self.connection_outboxes.get(conn_id)) |outbox| {
                    while (outbox.dequeue()) |envelope| {
                        log.debug("sending envelope.message to connection_outbox: {any}", .{envelope.message.fixed_headers});
                        worker.outbox.enqueue(envelope) catch {
                            outbox.prepend(envelope) catch unreachable;
                        };
                    }

                    if (outbox.isEmpty()) assert(self.removeConnectionOutbox(conn_id));
                }
            }
        }
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

        // create the connection
        // we are just gonna try to close this socket if anything blows up
        errdefer self.io.close_socket(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        const conn_id = kid.generate();
        conn.* = try Connection.init(
            conn_id,
            worker.io,
            socket,
            worker.allocator,
            worker.memory_pool,
            .{ .inbound = .{} },
        );
        errdefer conn.deinit();

        conn.connection_state = .connected;
        errdefer conn.connection_state = .closing;
        errdefer conn.protocol_state = .terminating;

        // create an auth_challenge for this connection
        const auth_challenge = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(auth_challenge);

        auth_challenge.* = Message.new(.auth_challenge);
        auth_challenge.ref();
        errdefer auth_challenge.deref();

        auth_challenge.extension_headers.auth_challenge.challenge_method = .token;
        // FIX: this should be prand at least
        auth_challenge.extension_headers.auth_challenge.nonce = @as(u128, kid.generate());
        auth_challenge.extension_headers.auth_challenge.connection_id = conn_id;

        conn.protocol_state = .authenticating;

        assert(auth_challenge.validate() == null);

        // create a handshake for this connection
        const handshake = Handshake{
            .nonce = auth_challenge.extension_headers.auth_challenge.nonce,
            .connection_id = conn_id,
            .challenge_method = auth_challenge.extension_headers.auth_challenge.challenge_method,
            .algorithm = auth_challenge.extension_headers.auth_challenge.algorithm,
        };

        try self.handshakes.put(self.allocator, conn.connection_id, handshake);
        errdefer _ = self.handshakes.remove(conn.connection_id);

        conn.outbox.enqueue(auth_challenge) catch @panic("connection does not have enough capacity in outbox");

        try worker.addInboundConnection(conn);
    }

    fn handleSessionInit(self: *Self, envelope: Envelope) !void {
        const conn_outbox = try self.findOrCreateConnectionOutbox(envelope.conn_id);

        // Ensure only one handshake per connection
        const entry = self.handshakes.fetchRemove(envelope.conn_id) orelse return error.HandshakeMissing;

        const handshake = entry.value;

        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        switch (handshake.challenge_method) {
            .token => {
                const message = envelope.message;
                if (self.authenticate(handshake, message)) {
                    // conn.protocol_state = .ready;

                    log.info("successfully authenticated (creating session)!", .{});
                    const session_init = message.extension_headers.session_init;

                    const session = try self.createSession(envelope.session_id, session_init.peer_id, session_init.peer_type);
                    errdefer self.removeSession(session.session_id);

                    try self.addConnectionToSession(session.session_id, envelope.conn_id);
                    errdefer _ = self.removeConnectionFromSession(session.session_id, envelope.conn_id);

                    reply.* = Message.new(.auth_success);

                    reply.extension_headers.auth_success.peer_id = session.peer_id;
                    reply.extension_headers.auth_success.session_id = session.session_id;
                    reply.setBody(session.session_token);
                } else {
                    reply.* = Message.new(.auth_failure);
                    reply.extension_headers.auth_failure.error_code = .unauthorized;
                }
            },
            else => @panic("unsupported challenge_method"),
        }

        reply.ref();
        errdefer reply.deref();

        const reply_envelope = Envelope{
            .conn_id = envelope.conn_id,
            .message = reply,
            .session_id = envelope.session_id,
            .message_id = kid.generate(),
        };

        try conn_outbox.enqueue(reply_envelope);
    }

    fn handleSessionJoin(self: *Self, envelope: Envelope) !void {
        const conn_outbox = try self.findOrCreateConnectionOutbox(envelope.conn_id);

        // Ensure only one handshake per connection
        const handshake_entry = self.handshakes.fetchRemove(envelope.conn_id) orelse return error.HandshakeMissing;
        const handshake = handshake_entry.value;

        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        switch (handshake.challenge_method) {
            .token => {
                if (self.authenticateWithSession(handshake, envelope.message)) {
                    log.info("successfully authenticated (joining session)!", .{});

                    const session_join_headers = envelope.message.extension_headers.session_join;

                    try self.addConnectionToSession(session_join_headers.session_id, envelope.conn_id);
                    errdefer _ = self.removeConnectionFromSession(session_join_headers.session_id, envelope.conn_id);

                    reply.* = Message.new(.auth_success);

                    reply.extension_headers.auth_success.peer_id = session_join_headers.peer_id;
                    reply.extension_headers.auth_success.session_id = session_join_headers.session_id;
                } else {
                    log.info("authentication unsuccessful", .{});

                    reply.* = Message.new(.auth_failure);

                    reply.extension_headers.auth_failure.error_code = .unauthorized;
                }
            },
            else => @panic("unsupported challenge_method"),
        }

        reply.ref();
        errdefer reply.deref();

        const reply_envelope = Envelope{
            .conn_id = envelope.conn_id,
            .message = reply,
            .session_id = envelope.session_id,
            .message_id = kid.generate(),
        };

        try conn_outbox.enqueue(reply_envelope);
    }

    fn handlePublish(self: *Self, envelope: Envelope) !void {
        const topic = try self.findOrCreateTopic(envelope.message.topicName(), .{});
        if (topic.queue.available() == 0) try topic.tick(); // if there is no space, try to advance the topic

        envelope.message.ref();
        topic.queue.enqueue(envelope) catch envelope.message.deref();

        // // if (@mod(self.metrics.messages_processed.load(.seq_cst), 1_000) == 0) {
        const received_at = std.time.nanoTimestamp();
        const created_at = std.fmt.parseInt(i128, envelope.message.body(), 10) catch 0;

        const diff = @divFloor(received_at - created_at, std.time.ns_per_us);
        log.info("took: {d}us", .{diff});
        // // }
    }

    fn handleSubscribe(self: *Self, envelope: Envelope) !void {
        const topic_name = envelope.message.topicName();
        const topic = try self.findOrCreateTopic(topic_name, .{});

        const reply = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(reply);

        reply.* = Message.new(.subscribe_ack);
        reply.extension_headers.subscribe_ack.transaction_id = envelope.message.extension_headers.subscribe.transaction_id;
        reply.extension_headers.subscribe_ack.error_code = .ok;

        reply.ref();
        errdefer reply.deref();

        const subscriber_key = utils.generateKey64(topic_name, envelope.session_id);
        _ = topic.addSubscriber(subscriber_key, envelope.session_id) catch |err| switch (err) {
            error.AlreadyExists => {
                log.err("subscriber already exists: {any}", .{err});
                reply.extension_headers.subscribe_ack.error_code = .failure;
            },
            else => {
                log.err("could not add subscriber: {any}", .{err});
                return err;
            },
        };

        const session_outbox = try self.findOrCreateSessionOutbox(envelope.session_id);

        const reply_envelope = Envelope{
            .conn_id = envelope.conn_id,
            .message = reply,
            .message_id = kid.generate(),
            .session_id = envelope.session_id,
        };

        log.info("subscribion to topic {s} added for session: {}", .{ topic_name, envelope.session_id });

        try session_outbox.enqueue(reply_envelope);
    }

    fn authenticate(self: *Self, handshake: Handshake, message: *Message) bool {
        const auth_token_config = self.config.authenticator_config.token;

        const session_init = message.extension_headers.session_init;
        switch (session_init.peer_type) {
            .client => {
                if (auth_token_config.clients) |client_token_entries| {
                    if (self.findClientToken(client_token_entries, session_init.peer_id)) |token_entry| {
                        return switch (handshake.algorithm) {
                            .hmac256 => self.verifyHMAC256(token_entry.token, handshake.nonce, message.body()),
                            else => @panic("unsupported algorithm"),
                        };
                    } else {
                        log.err("could not authenticate", .{});
                        return false;
                    }
                }
            },
            .node => @panic("unsupported broker type"),
        }

        return false;
    }

    fn authenticateWithSession(self: *Self, handshake: Handshake, message: *Message) bool {
        const session_opt = self.getSession(message.extension_headers.session_join.session_id);
        if (session_opt == null) return false;

        const session = session_opt.?;

        if (session.peer_id != message.extension_headers.session_join.peer_id) return false;
        if (session.session_id != message.extension_headers.session_join.session_id) return false;

        return switch (handshake.algorithm) {
            .hmac256 => self.verifyHMAC256(session.session_token, handshake.nonce, message.body()),
            else => |algorithm| {
                log.err("use of unsupported algorithm {any}", .{algorithm});
                return false;
            },
        };
    }

    fn findClientToken(_: *Self, clients: []const TokenEntry, peer_id: u64) ?TokenEntry {
        for (clients) |token_entry| if (token_entry.id == peer_id) return token_entry;
        return null;
    }

    fn verifyHMAC256(_: *Self, token: []const u8, nonce: u128, challenge_payload: []const u8) bool {
        const HMAC = std.crypto.auth.hmac.sha2.HmacSha256;
        var out: [HMAC.mac_length]u8 = undefined;
        var nonce_buf: [@sizeOf(u128)]u8 = undefined;

        std.mem.writeInt(u128, &nonce_buf, nonce, .big);

        var hmac = HMAC.init(token);
        hmac.update(&nonce_buf);
        hmac.final(&out);

        return std.mem.eql(u8, challenge_payload, &out);
    }

    fn initializeWorkers(self: *Self) !void {
        assert(self.workers.count() == 0);

        for (0..self.config.worker_threads) |id| {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);

            worker.* = try Worker.init(self.allocator, id, self.memory_pool);
            errdefer worker.deinit();

            try self.workers.put(self.allocator, id, worker);
            errdefer _ = self.workers.remove(id);

            switch (self.workers_load_balancer) {
                .round_robin => |*lb| try lb.addItem(self.allocator, id),
            }
        }

        if (self.config.listener_configs) |listener_configs| {
            for (listener_configs, 0..listener_configs.len) |listener_config, id| {
                switch (self.workers_load_balancer) {
                    .round_robin => |*lb| {
                        if (lb.next()) |worker_id| {
                            const worker = self.workers.get(worker_id).?;
                            worker.listeners_mutex.lock();
                            defer worker.listeners_mutex.unlock();

                            switch (listener_config.transport) {
                                .tcp => {
                                    const tcp_listener = try self.allocator.create(Listener);
                                    errdefer self.allocator.destroy(tcp_listener);

                                    tcp_listener.* = try Listener.init(self.allocator, self.io, id, listener_config);
                                    errdefer tcp_listener.deinit();

                                    try worker.listeners.put(worker.allocator, id, tcp_listener);
                                },
                            }
                        }
                    },
                }
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

                        tcp_listener.* = try Listener.init(self.allocator, self.io, id, listener_config);
                        errdefer tcp_listener.deinit();

                        try self.listeners.put(self.allocator, id, tcp_listener);
                    },
                }
            }
        }
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

    fn createSession(self: *Self, session_id: u64, peer_id: u64, peer_type: PeerType) !*Session {
        // self.sessions_mutex.lock();
        // defer self.sessions_mutex.unlock();

        const session = try self.allocator.create(Session);
        errdefer self.allocator.destroy(session);

        session.* = try Session.init(self.allocator, session_id, peer_id, peer_type, .round_robin);
        errdefer session.deinit(self.allocator);

        try self.sessions.put(self.allocator, session_id, session);

        return session;
    }

    fn removeSession(self: *Self, session_id: u64) void {
        // self.sessions_mutex.lock();
        // defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            session.deinit(self.allocator);
            _ = self.sessions.remove(session_id);
        }
    }

    fn getSession(self: *Self, session_id: u64) ?*Session {
        // self.sessions_mutex.lock();
        // defer self.sessions_mutex.unlock();

        return self.sessions.get(session_id);
    }

    fn addConnectionToSession(self: *Self, session_id: u64, conn_id: u64) !void {
        // self.sessions_mutex.lock();
        // defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            try session.addConnection(self.allocator, conn_id);
        } else {
            return error.SessionNotFound;
        }
    }

    fn removeConnectionFromSession(self: *Self, session_id: u64, conn_id: u64) bool {
        // self.sessions_mutex.lock();
        // defer self.sessions_mutex.unlock();

        if (self.sessions.get(session_id)) |session| {
            return session.removeConnection(conn_id);
        }
        return false;
    }

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

            queue.* = try RingBuffer(Envelope).init(self.allocator, 5);
            errdefer queue.deinit();

            try self.connection_outboxes.put(self.allocator, conn_id, queue);
            connection_outbox = queue;
        }

        return connection_outbox;
    }

    fn removeConnectionOutbox(self: *Self, conn_id: u64) bool {
        if (self.connection_outboxes.get(conn_id)) |queue| {
            while (queue.dequeue()) |envelope| {
                const message = envelope.message;
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            queue.deinit();
            self.allocator.destroy(queue);
            return self.connection_outboxes.remove(conn_id);
        }

        return false;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var broker = try Broker.init(allocator, .{});
    defer broker.deinit();

    try broker.start();
    defer broker.close();
}

// broker tick
// 1. gather envelopes from workers
// 2. if envelope is not connected to a session, then pass it to the broker
//

// Da Rulez:
// 1. in order for a broker to connect to another broker, one of those brokers must be listening on a port. That broker
// 2. broker can have `n` connections per session. A session can only be connected to a single broker
// 3. a broker can dynamically listen and unlisten for new connection. Unlistening from connections does not disconnect
//     connections that have already been established through that listening port.
// 4. brokers are simple mediums to communicate with eachother. A broker is not a router. A broker is both a client and a
//     server with `n` symetrical connections to another broker.
// 5.
//
// const broker = try broker.init(allocator, config)
// defer broker.deinit();
//
// spawn a background thread to handle all the io and stuff.
// try broker.start();
// defer broker.close(); // close will destroy all subscriptions, advertisments and all that ungracefully so best to do it deliberatly
//
// // if there was a listener that wasn't spawned w/ the broker_config, then spawn a separate one here. Should
// spawn a new thread per listener
// const listener_id = try broker.listen(listener_config)
// defer broker.unlisten(listener_id);
//
// // create a session and initialize the outbound connection
// // this is a synchronous connect
// const session_id = try broker.connect(outbound_config)
// try broker.awaitConnected(session_id, timeout_ns);
//
// // disconnect all connections associated with this session
// defer broker.disconnect(session_id);
//
// // subscribe to this topic with this session. Sessions can only be connected to a single "broker" so this is pretty safe
// const callback_id = broker.subscribe(session_id, topic_name, callback);
//
// // unsubscribe this function from this session
// defer _ = broker.unsubscribe(session_id, callback_id);
//
//
// try broker.publish(session_id, topic, body, opts)
