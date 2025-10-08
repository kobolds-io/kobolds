const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
const posix = std.posix;
const atomic = std.atomic;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const KID = @import("kid").KID;

const IO = @import("../io.zig").IO;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const Signal = @import("stdx").Signal;
const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;

const ClientTopic = @import("../pubsub/client_topic.zig").ClientTopic;
const ClientTopicOptions = @import("../pubsub/client_topic.zig").ClientTopicOptions;

// const Service = @import("../services/service.zig").Service;
// const Advertiser = @import("../services/advertiser.zig").Advertiser;
// const Requestor = @import("../services/requestor.zig").Requestor;
const AdvertiserCallback = *const fn (request: *Message, reply: *Message) void;

const Session = @import("../node/session.zig").Session;

const PingOptions = struct {};
const PublishOptions = struct {};
const SubscribeOptions = struct {
    timeout_ms: u64 = 5_000,
};
const UnsubscribeOptions = struct {
    timeout_ms: u64 = 5_000,
};
const UnadvertiseOptions = struct {
    timeout_ms: u64 = 5_000,
};
const RequestOptions = struct {
    timeout_ms: u64 = 5_000,
};
const AdvertiseOptions = struct {};

pub const AuthenticationConfig = struct {
    token_config: ?TokenAuthConfig = null,
};

pub const TokenAuthConfig = struct {
    id: u64,
    token: []const u8,
};

pub const ClientConfig = struct {
    client_id: u11 = 1,
    host: []const u8 = "127.0.0.1",
    port: u16 = 8000,
    max_connections: u16 = 10,
    min_connections: u16 = 3,
    memory_pool_capacity: usize = constants.default_client_memory_pool_capacity,
    authentication_config: AuthenticationConfig = .{},
};

const ClientState = enum {
    running,
    closing,
    closed,
};

const ClientMetrics = struct {
    bytes_recv_total: u128 = 0,
    bytes_send_total: u128 = 0,
    messages_recv_total: u128 = 0,
    messages_send_total: u128 = 0,
};

pub const Client = struct {
    const Self = @This();

    // advertiser_callbacks: std.AutoHashMap(u128, AdvertiserCallback),
    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    config: ClientConfig,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(u64, *Connection),
    done_channel: *UnbufferedChannel(bool),
    id: u11,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(*Message),
    io: *IO,
    kid: KID,
    memory_pool: *MemoryPool(Message),
    metrics: ClientMetrics,
    mutex: std.Thread.Mutex,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(*Message),
    // services_mutex: std.Thread.Mutex,
    // services: std.StringHashMap(*ClientService),
    session_mutex: std.Thread.Mutex,
    session: ?*Session = null,
    state: ClientState,
    topics_mutex: std.Thread.Mutex,
    topics: std.StringHashMap(*ClientTopic),
    transactions_mutex: std.Thread.Mutex,
    transactions: std.AutoHashMapUnmanaged(u64, *Transaction),
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) !Self {
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

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, constants.default_client_inbox_capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(*Message).init(allocator, constants.default_client_outbox_capacity);
        errdefer outbox.deinit();

        return Self{
            // .advertiser_callbacks = std.AutoHashMap(u128, AdvertiserCallback).init(allocator),
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(u64, *Connection).init(allocator),
            .done_channel = done_channel,
            .id = config.client_id,
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .outbox_mutex = std.Thread.Mutex{},
            .io = io,
            .kid = KID.init(config.client_id, .{}),
            .memory_pool = memory_pool,
            .metrics = ClientMetrics{},
            .mutex = std.Thread.Mutex{},
            // .services_mutex = std.Thread.Mutex{},
            // .services = std.StringHashMap(*ClientService).init(allocator),
            .session_mutex = std.Thread.Mutex{},
            .session = null,
            .state = .closed,
            .topics_mutex = std.Thread.Mutex{},
            .topics = std.StringHashMap(*ClientTopic).init(allocator),
            .transactions_mutex = std.Thread.Mutex{},
            .transactions = .empty,
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iterator = self.connections.valueIterator();
        while (connections_iterator.next()) |entry| {
            const connection = entry.*;

            assert(connection.connection_state == .closed);

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var uninitialized_connections_iterator = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iterator.next()) |entry| {
            const connection = entry.*;

            assert(connection.connection_state == .closed);

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var topics_iterator = self.topics.valueIterator();
        while (topics_iterator.next()) |entry| {
            const topic = entry.*;

            self.allocator.free(topic.topic_name);

            topic.deinit();
            self.allocator.destroy(topic);
        }

        var transactions_iterator = self.transactions.valueIterator();
        while (transactions_iterator.next()) |entry| {
            const transaction = entry.*;

            self.allocator.destroy(transaction.signal);
            self.allocator.destroy(transaction);
        }

        // var services_iterator = self.services.valueIterator();
        // while (services_iterator.next()) |entry| {
        //     const service = entry.*;

        //     self.allocator.free(service.topic_name);

        //     service.deinit();
        //     self.allocator.destroy(service);
        // }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        while (self.outbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        if (self.session) |session| {
            session.deinit(self.allocator);
            self.allocator.destroy(session);
        }

        self.io.deinit();
        self.memory_pool.deinit();
        self.connections.deinit();
        self.uninitialized_connections.deinit();
        self.transactions.deinit(self.allocator);
        self.topics.deinit();
        // self.services.deinit();
        // self.advertiser_callbacks.deinit();
        self.inbox.deinit();
        self.outbox.deinit();

        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);
    }

    pub fn start(self: *Self) !void {
        // Start the client thread
        var ready_channel = UnbufferedChannel(bool).new();
        const client_thread = try std.Thread.spawn(.{}, Client.run, .{ self, &ready_channel });
        client_thread.detach();

        _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("client_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn run(self: *Client, ready_channel: *UnbufferedChannel(bool)) void {
        self.state = .running;
        ready_channel.send(true);
        log.info("client {} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("client {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;

                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch |err| {
                        // self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                        log.err("client failed to run io {any}", .{err});
                    };
                },
                .closing => {
                    log.info("client {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    // FIX: this is broken somehow, where the loop isn't killing the connections
    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                while (!self.closeAllConnections()) {}

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
    }

    pub fn awaitConnected(self: *Self, timeout_ns: u64) void {
        // if we are already connected, do nothing
        if (self.isConnected()) return;

        var now = std.time.nanoTimestamp();
        const deadline = now + timeout_ns;
        while (now < deadline) {
            if (self.isConnected()) return;

            std.Thread.sleep(1 * std.time.ns_per_ms);
            now = std.time.nanoTimestamp();
        }
    }

    pub fn drain(self: *Self, timeout_ns: u64) void {
        var now = std.time.nanoTimestamp();
        const deadline = now + timeout_ns;

        dealine_loop: while (deadline > now) {
            const memory_pool_drained = self.memory_pool.available() == self.memory_pool.capacity;

            self.outbox_mutex.lock();
            defer self.outbox_mutex.unlock();

            self.inbox_mutex.lock();
            defer self.inbox_mutex.unlock();

            self.connections_mutex.lock();
            defer self.connections_mutex.unlock();

            var connections_iter = self.connections.valueIterator();
            while (connections_iter.next()) |entry| {
                const conn = entry.*;
                if (conn.inbox.count > 0) continue :dealine_loop;
                if (conn.outbox.count > 0) continue :dealine_loop;
            }

            if (memory_pool_drained and self.outbox.isEmpty() and self.inbox.isEmpty()) return;

            log.info("draining!", .{});
            std.Thread.sleep(100 * std.time.ns_per_ms);
            now = std.time.nanoTimestamp();
        } else {
            log.warn("drain timed out", .{});
        }
        // always sleep a little longer
        std.Thread.sleep(1_000 * std.time.ns_per_ms);
    }

    fn tick(self: *Self) !void {
        try self.initializeOutboundConnections();
        try self.tickConnections();
        try self.processInboundMessages();
        try self.processOutboundMessages();
    }

    fn tickConnections(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;

            // check if this connection was closed for whatever reason
            if (conn.connection_state == .closed) {
                try self.cleanupConnection(conn);
                continue;
            }

            conn.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };
        }
    }

    fn initializeOutboundConnections(self: *Self) !void {
        // figure out how many connections we currently have
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.*;
            // ensure that all of them are past authenticating before creating another connection
            if (conn.protocol_state == .authenticating) return;
        }

        if (self.connections.count() < self.config.min_connections) {

            // we need to create a new connection
            try self.createOutboundConnection();
            return;
        }
    }

    fn processInboundMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |connection_entry| {
            const conn = connection_entry.*;

            if (conn.inbox.isEmpty()) continue;
            while (conn.inbox.dequeue()) |message| {

                // if this message has more than a single ref, something has not been initialized or deinitialized correctly.
                assert(message.refs() == 1);

                switch (message.fixed_headers.message_type) {
                    .auth_challenge => {
                        // NOTE: we do some swapping of the connections hashmap in this function
                        // as we swap the temporary id to the actual conn.connection_id from the node
                        try self.handleAuthChallenge(conn, message);

                        // reset the connections_iter
                        connections_iter = self.connections.valueIterator();
                    },
                    .auth_success => try self.handleAuthSuccess(conn, message),
                    .auth_failure => try self.handleAuthFailure(conn, message),
                    else => {
                        self.inbox.enqueue(message) catch conn.inbox.prepend(message) catch unreachable;
                        // log.info("message.fixed_headers.message_type {any}", .{message.fixed_headers.message_type});
                    },
                }
            }
        }

        while (self.inbox.dequeue()) |message| {
            defer {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            switch (message.fixed_headers.message_type) {
                .publish => try self.handlePublish(message),
                .subscribe_ack => try self.handleSubscribeAck(message),
                .unsubscribe_ack => try self.handleUnsubscribeAck(message),
                else => @panic("unhandle message type!"),
            }
        }

        assert(self.inbox.isEmpty());

        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;

            try topic.tick();
        }
    }

    fn processOutboundMessages(self: *Self) !void {
        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        self.session_mutex.lock();
        defer self.session_mutex.unlock();

        if (self.session == null) return;
        const session = self.session.?;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        log.info("enqueing {} messages in connections", .{self.outbox.count});

        while (self.outbox.dequeue()) |message| {
            assert(message.refs() == 1);

            const conn = session.getNextConnection();

            conn.outbox.enqueue(message) catch {
                self.outbox.prepend(message) catch unreachable;
                return;
            };
        }
    }

    fn handleAuthSuccess(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        if (self.session) |session| {
            try session.addConnection(self.allocator, conn);

            conn.protocol_state = .ready;

            log.info("successfully authenticated (joined session)!", .{});
        } else {
            const session = try self.allocator.create(Session);
            errdefer self.allocator.destroy(session);

            const peer_id = message.extension_headers.auth_success.peer_id;
            const session_id = message.extension_headers.auth_success.session_id;

            session.* = try Session.init(self.allocator, session_id, peer_id, .node, .round_robin);
            errdefer session.deinit(self.allocator);

            assert(session.session_token.len == message.fixed_headers.body_length);
            @memcpy(session.session_token, message.body());

            try session.addConnection(self.allocator, conn);
            errdefer session.removeConnection(conn.connection_id);

            self.session = session;

            conn.protocol_state = .ready;

            log.info("successfully authenticated (new session)!", .{});
        }
    }

    fn handleAuthFailure(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        log.info("auth failure: {any}, closing connection {}", .{
            message.extension_headers.auth_failure.error_code,
            conn.connection_id,
        });

        conn.connection_state = .closing;
        conn.protocol_state = .terminating;
    }

    fn handleAuthChallenge(self: *Self, conn: *Connection, message: *Message) !void {
        // we should totally crash because this is a sequencing issue
        assert(conn.protocol_state == .authenticating);

        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        // we need to swap the ID of this connection
        const tmp_conn_id = conn.connection_id;
        defer _ = self.connections.remove(tmp_conn_id);

        // we immediately assign this connection the id it receives from the node
        conn.connection_id = message.extension_headers.auth_challenge.connection_id;

        try self.connections.put(conn.connection_id, conn);
        errdefer _ = self.connections.remove(conn.connection_id);

        const session_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(session_message);

        if (self.session) |session| {
            session_message.* = Message.new(.session_join);
            session_message.extension_headers.session_join.peer_id = session.peer_id;
            session_message.extension_headers.session_join.session_id = session.session_id;

            switch (message.extension_headers.auth_challenge.challenge_method) {
                .token => {
                    switch (message.extension_headers.auth_challenge.algorithm) {
                        .hmac256 => {
                            const HMAC = std.crypto.auth.hmac.sha2.HmacSha256;
                            var out: [HMAC.mac_length]u8 = undefined;
                            var nonce_slice: [@sizeOf(u128)]u8 = undefined;

                            // convert the nonce to a slice
                            std.mem.writeInt(u128, &nonce_slice, message.extension_headers.auth_challenge.nonce, .big);

                            var hmac = HMAC.init(session.session_token);
                            hmac.update(&nonce_slice);
                            hmac.final(&out);

                            session_message.setBody(&out);
                            log.info("attempting to join session", .{});
                        },
                        else => @panic("unimplemented algorithm"),
                    }
                },
                else => @panic("unsupported challenge_method"),
            }
        } else {
            session_message.* = Message.new(.session_init);
            session_message.extension_headers.session_init.peer_type = .client;

            switch (message.extension_headers.auth_challenge.challenge_method) {
                .token => {
                    if (self.config.authentication_config.token_config) |token_config| {
                        // Set the peer id
                        session_message.extension_headers.session_init.peer_id = token_config.id;

                        switch (message.extension_headers.auth_challenge.algorithm) {
                            .hmac256 => {
                                const HMAC = std.crypto.auth.hmac.sha2.HmacSha256;
                                var out: [HMAC.mac_length]u8 = undefined;
                                var nonce_slice: [@sizeOf(u128)]u8 = undefined;

                                // convert the nonce to a slice
                                std.mem.writeInt(u128, &nonce_slice, message.extension_headers.auth_challenge.nonce, .big);

                                var hmac = HMAC.init(token_config.token);
                                hmac.update(&nonce_slice);
                                hmac.final(&out);

                                session_message.setBody(&out);
                            },
                            else => @panic("unimplemented algorithm"),
                        }
                    } else {
                        return error.TokenConfigMissing;
                    }
                },
                else => @panic("unsupported challenge_method"),
            }
        }

        assert(session_message.validate() == null);

        session_message.ref();
        errdefer session_message.deref();

        try conn.outbox.enqueue(session_message);
    }

    fn handleSubscribeAck(self: *Self, message: *Message) !void {
        assert(message.refs() == 1);

        self.transactions_mutex.lock();
        defer self.transactions_mutex.unlock();

        if (self.transactions.get(message.extension_headers.subscribe_ack.transaction_id)) |transaction| {
            message.ref();

            transaction.signal.send(message);
        } else {
            log.warn("received reply for unhandled transaction {}. discarding message", .{message.extension_headers.subscribe_ack.transaction_id});
            return;
        }
    }

    fn handleUnsubscribeAck(self: *Self, message: *Message) !void {
        log.info("handle UnsubscribeAck", .{});

        assert(message.refs() == 1);

        self.transactions_mutex.lock();
        defer self.transactions_mutex.unlock();

        if (self.transactions.get(message.extension_headers.unsubscribe_ack.transaction_id)) |transaction| {
            message.ref();

            transaction.signal.send(message);
        } else {
            log.warn("received reply for unhandled transaction {}. discarding message", .{message.extension_headers.subscribe_ack.transaction_id});
            return;
        }
    }

    // FIX: this function looks up the topic and serially executes the callbacks. It doesn't allow the topic to do
    // any message ordering enforcement like it should. as part of Topic.tick() the topic should sort messages.
    fn handlePublish(self: *Self, message: *Message) !void {
        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();
        // log.info("here {any}, topic_count: {}", .{ message.topicName(), self.topics.count() });

        if (self.topics.get(message.topicName())) |topic| {

            // if the topic is already full, then just execute all the callbacks for this topic
            if (topic.queue.isFull()) {
                try topic.tick();
                assert(topic.queue.isEmpty());
            }

            // var callbacks_iter = topic.callbacks.valueIterator();
            // while (callbacks_iter.next()) |entry| {
            //     const callback = entry.*;
            //     callback(message);
            // }
            message.ref();
            try topic.queue.enqueue(message);
        } else {
            log.warn("topic not found", .{});
        }
    }

    fn handleServiceRequest(self: *Self, conn: *Connection, message: *Message) !void {
        _ = conn;

        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        _ = self.connections.remove(conn.connection_id);

        log.info("client: {} removed connection {}", .{ self.id, conn.connection_id });

        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn closeAllConnections(self: *Self) bool {
        var all_connections_closed = true;

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |entry| {
            var conn = entry.*;
            switch (conn.connection_state) {
                .closed => continue,
                .closing => {
                    all_connections_closed = false;
                },
                else => {
                    conn.connection_state = .closing;
                    conn.protocol_state = .terminating;
                    all_connections_closed = false;
                },
            }
        }

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            var conn = entry.*;
            switch (conn.connection_state) {
                .closed => continue,
                .closing => {
                    all_connections_closed = false;
                },
                else => {
                    conn.connection_state = .closing;
                    conn.protocol_state = .terminating;
                    all_connections_closed = false;
                },
            }
        }

        return all_connections_closed;
    }

    fn createOutboundConnection(self: *Self) !void {
        // create the socket
        const address = try std.net.Address.parseIp4(self.config.host, self.config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const socket = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        // create a temporary id that will be used to identify this connection until it receives a proper
        // connection_id from the remote node
        const tmp_conn_id = self.kid.generate();
        conn.* = try Connection.init(
            tmp_conn_id,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
            .{
                .outbound = .{
                    .host = self.config.host,
                    .port = self.config.port,
                },
            },
        );
        errdefer conn.deinit();

        conn.connection_state = .connecting;
        conn.protocol_state = .authenticating;

        try self.connections.put(tmp_conn_id, conn);
        errdefer self.connections.remove(tmp_conn_id);

        self.io.connect(
            *Connection,
            conn,
            Connection.onConnect,
            conn.connect_completion,
            socket,
            address,
        );
        conn.connect_submitted = true;
    }

    pub fn isConnected(self: *Self) bool {
        self.session_mutex.lock();
        defer self.session_mutex.unlock();

        if (self.session) |session| {
            self.connections_mutex.lock();
            defer self.connections_mutex.unlock();

            var connections_iter = session.connections.valueIterator();
            while (connections_iter.next()) |entry| {
                const conn = entry.*;
                if (conn.connection_state == .connected and conn.protocol_state == .ready) return true;
            }

            return false;
        } else return false;
    }

    pub fn publish(self: *Self, topic_name: []const u8, body: []const u8, _: PublishOptions) !void {
        if (!self.isConnected()) return error.NotConnected;

        const message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(message);

        message.* = Message.new(.publish);
        message.ref();
        errdefer message.deref();

        message.setTopicName(topic_name);
        message.setBody(body);

        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        // log.info("publish message.topicName(): {any}", .{message.topicName()});
        // log.info("publish message.body(): {any}", .{message.body()});

        try self.outbox.enqueue(message);
    }

    pub fn subscribe(self: *Self, topic_name: []const u8, callback: ClientTopic.Callback, options: SubscribeOptions) !u64 {
        if (!self.isConnected()) return error.NotConnected;

        var timer = try std.time.Timer.start();
        const timer_start = timer.read();

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        const client_topic = try self.findOrCreateClientTopic(topic_name, .{});

        const callback_id = self.kid.generate();
        try client_topic.addCallback(callback_id, callback);
        errdefer _ = client_topic.removeCallback(callback_id);

        // this topic is already subscribed on the node. Therefore we are just adding a callback to this
        // existing topic and no extra work needs to be done.
        if (client_topic.callbacks.count() > 1) {
            const diff = (timer.read() - timer_start) / std.time.ns_per_us;
            log.info("subscribed to topic {s}. took {}us", .{ topic_name, diff });
            return callback_id;
        }

        const transaction_id = self.kid.generate();

        const subscribe_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(subscribe_message);

        subscribe_message.* = Message.new(.subscribe);
        subscribe_message.setTopicName(client_topic.topic_name);
        subscribe_message.extension_headers.subscribe.transaction_id = transaction_id;
        subscribe_message.ref();
        errdefer subscribe_message.deref();

        assert(subscribe_message.validate() == null);

        const reply = try self.request(transaction_id, subscribe_message, .{ .timeout_ms = options.timeout_ms });
        defer {
            reply.deref();
            if (reply.refs() == 0) self.memory_pool.destroy(reply);
        }

        switch (reply.extension_headers.subscribe_ack.error_code) {
            .ok => {
                const diff = (timer.read() - timer_start) / std.time.ns_per_us;
                log.info("subscribed to topic {s}. took {}us", .{ topic_name, diff });

                return callback_id;
            },
            else => return error.FailedToSubscribe,
        }
    }

    pub fn unsubscribe(self: *Self, topic_name: []const u8, callback_id: u64, options: UnsubscribeOptions) !void {
        var timer = try std.time.Timer.start();
        const timer_start = timer.read();

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();
        const client_topic = try self.findOrCreateClientTopic(topic_name, .{});

        if (!client_topic.removeCallback(callback_id)) {
            return error.CallbackMissing;
        }

        // there is no more work to be done if this is the case as there is another callback that is still registered.
        if (client_topic.callbacks.count() > 0) {
            const diff = (timer.read() - timer_start) / std.time.ns_per_us;
            log.info("unsubscribed from topic {s}. took {}us", .{ topic_name, diff });
            return;
        }

        const transaction_id = self.kid.generate();

        const unsubscribe_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(unsubscribe_message);

        unsubscribe_message.* = Message.new(.unsubscribe);
        unsubscribe_message.setTopicName(client_topic.topic_name);
        unsubscribe_message.extension_headers.unsubscribe.transaction_id = transaction_id;
        unsubscribe_message.ref();
        errdefer unsubscribe_message.deref();

        assert(unsubscribe_message.validate() == null);

        const reply = try self.request(transaction_id, unsubscribe_message, .{ .timeout_ms = options.timeout_ms });
        defer {
            reply.deref();
            if (reply.refs() == 0) self.memory_pool.destroy(reply);
        }

        switch (reply.extension_headers.unsubscribe_ack.error_code) {
            .ok => {
                const diff = (timer.read() - timer_start) / std.time.ns_per_us;
                log.info("unsubscribed from topic {s}. took {}us", .{ topic_name, diff });

                return;
            },
            else => return error.FailedToUnsubscribe,
        }
    }

    pub fn advertise(self: *Self, topic_name: []const u8, callback: AdvertiserCallback, options: AdvertiseOptions) !u64 {
        if (!self.isConnected()) return error.NotConnected;

        // var timer = try std.time.Timer.start();
        // const timer_start = timer.read();

        // self.topics_mutex.lock();
        // defer self.topics_mutex.unlock();

        // const client_topic = try self.findOrCreateClientTopic(topic_name, .{});

        // const callback_id = self.kid.generate();
        // try client_topic.addCallback(callback_id, callback);
        // errdefer _ = client_topic.removeCallback(callback_id);

        // const diff = timer.read() - timer_start;
        // _ = diff;
        _ = options;
        _ = topic_name;
        _ = callback;

        return 0;
    }

    pub fn unadvertise(self: *Self, topic_name: []const u8, callback_id: u64, options: UnadvertiseOptions) !void {
        _ = self;
        _ = callback_id;
        _ = options;
        _ = topic_name;
    }

    fn request(self: *Self, transaction_id: u64, request_message: *Message, options: RequestOptions) !*Message {
        // create the signal handler
        var signal = Signal(*Message).new();

        // create a transaction
        const transaction = try self.allocator.create(Transaction);
        errdefer self.allocator.destroy(transaction);

        transaction.* = Transaction{
            .id = transaction_id,
            .timeout_ms = options.timeout_ms,
            .created_at = @intCast(std.time.milliTimestamp()),
            .signal = &signal,
        };

        // add this transaction to the transactions map
        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(self.allocator, transaction.id, transaction);
            errdefer _ = self.transactions.remove(transaction.id);
        }

        // add this message the outbox
        {
            self.outbox_mutex.lock();
            defer self.outbox_mutex.unlock();

            // enqueue the request in the outbox
            try self.outbox.enqueue(request_message);
        }

        // wait for the transaction to be completed
        const reply_message = try signal.tryReceive(options.timeout_ms * std.time.ns_per_ms);

        // cleanup
        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            assert(self.transactions.remove(transaction.id));
            self.allocator.destroy(transaction);
        }

        return reply_message;
    }

    fn findOrCreateClientTopic(self: *Self, topic_name: []const u8, options: ClientTopicOptions) !*ClientTopic {
        _ = options;

        var topic: *ClientTopic = undefined;
        if (self.topics.get(topic_name)) |t| {
            topic = t;
        } else {
            topic = try self.allocator.create(ClientTopic);
            errdefer self.allocator.destroy(topic);

            const t_name = try self.allocator.alloc(u8, topic_name.len);
            errdefer self.allocator.free(t_name);

            @memcpy(t_name, topic_name);

            topic.* = try ClientTopic.init(self.allocator, self.memory_pool, t_name, .{});
            errdefer topic.deinit();

            try self.topics.put(t_name, topic);
        }

        return topic;
    }
};

pub const Transaction = struct {
    id: u64,
    timeout_ms: u64,
    created_at: u64,
    signal: *Signal(*Message),
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();
}
