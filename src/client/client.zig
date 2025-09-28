const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const KID = @import("kid").KID;

const IO = @import("../io.zig").IO;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const Signal = @import("stdx").Signal;
const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message2.zig").Message;
const Connection = @import("../protocol/connection2.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;

const ClientTopic = @import("../pubsub/client_topic.zig").ClientTopic;
// const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
// const SubscriberCallback = *const fn (message: *Message) void;

const Service = @import("../services/service.zig").Service;
const Advertiser = @import("../services/advertiser.zig").Advertiser;
const Requestor = @import("../services/requestor.zig").Requestor;
const AdvertiserCallback = *const fn (request: *Message, reply: *Message) void;

const Session = @import("../node/session.zig").Session;

const PingOptions = struct {};
const PublishOptions = struct {};
const SubscribeOptions = struct {};
const RequestOptions = struct {};
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

    advertiser_callbacks: std.AutoHashMap(u128, AdvertiserCallback),
    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    config: ClientConfig,
    connection_messages: ConnectionMessages,
    connection_messages_mutex: std.Thread.Mutex,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(u64, *Connection),
    done_channel: *UnbufferedChannel(bool),
    id: u11,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(*Message),
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(*Message),
    io: *IO,
    kid: KID,
    memory_pool: *MemoryPool(Message),
    metrics: ClientMetrics,
    mutex: std.Thread.Mutex,
    services_mutex: std.Thread.Mutex,
    services: std.StringHashMap(*Service),
    session_mutex: std.Thread.Mutex,
    session: ?*Session = null,
    state: ClientState,
    topics_mutex: std.Thread.Mutex,
    topics: std.StringHashMap(*ClientTopic),
    transactions_mutex: std.Thread.Mutex,
    transactions: std.AutoHashMap(uuid.Uuid, *Signal(*Message)),
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
            .advertiser_callbacks = std.AutoHashMap(u128, AdvertiserCallback).init(allocator),
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            .connection_messages = ConnectionMessages.init(allocator, memory_pool),
            .connection_messages_mutex = std.Thread.Mutex{},
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
            .services_mutex = std.Thread.Mutex{},
            .services = std.StringHashMap(*Service).init(allocator),
            .session_mutex = std.Thread.Mutex{},
            .session = null,
            .state = .closed,
            .topics_mutex = std.Thread.Mutex{},
            .topics = std.StringHashMap(*ClientTopic).init(allocator),
            .transactions_mutex = std.Thread.Mutex{},
            .transactions = std.AutoHashMap(uuid.Uuid, *Signal(*Message)).init(allocator),
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.connection_messages.deinit();

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

            topic.deinit();
            self.allocator.destroy(topic);
        }

        var services_iterator = self.services.valueIterator();
        while (services_iterator.next()) |entry| {
            const service = entry.*;

            service.deinit();
            self.allocator.destroy(service);
        }

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
        self.transactions.deinit();
        self.topics.deinit();
        self.services.deinit();
        self.advertiser_callbacks.deinit();
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

    /// initialize a session with the node. The client will handle scaling connections up to the maximum
    pub fn connect2(self: *Self, timeout_ns: u64) !void {
        _ = timeout_ns;
        _ = self;

        // We need to be able to create a session

        // // TODO: check if we have a session
        // if (self.session) |session| {
        //     self.session_mutex.lock();
        //     defer self.session_mutex.unlock();

        //     // we already have a session with an active connection, we should do nothing.
        //     if (session.connections.count() > 0) return;

        //     // we are in a transition state where the session hasn't been deinitialized. Let's do that now.
        //     session.deinit(self.allocator);
        //     self.allocator.destroy(session);
        //     self.session = null;
        // }

        // const ready_signal = try self.initiateOutboundConnection();
        // defer self.allocator.destroy(ready_signal);

        // ready_channel.tryReceive(timeout_ns: u64)

        // try self.spawnConnection();

        // self.connections_mutex.lock();
        // defer self.connections_mutex.unlock();

        // Try to create a session!

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

    pub fn drain(self: *Self) void {
        while (self.memory_pool.available() != self.memory_pool.capacity) {}
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    fn tick(self: *Self) !void {
        try self.tickConnections();
        try self.initializeOutboundConnections();
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

            if (conn.inbox.count == 0) continue;
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
                    .publish => try self.handlePublish(conn, message),
                    .subscribe_ack => try self.handleSubscribeAck(conn, message),
                    else => {
                        log.info("message.fixed_headers.message_type {any}", .{message.fixed_headers.message_type});
                    },
                    // else => unreachable,
                }
            }
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

    fn handleSubscribeAck(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        log.info("handleSubscribeAck: conn_id {}", .{conn.connection_id});
    }

    fn handlePublish(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        log.err("handlePublish: conn_id {}", .{conn.connection_id});
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

        try self.outbox.enqueue(message);
    }

    pub fn subscribe(self: *Self, topic_name: []const u8, callback: ClientTopic.Callback, _: SubscribeOptions) !u64 {
        if (!self.isConnected()) return error.NotConnected;

        // create a subscription callback entry

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        var topic: *ClientTopic = undefined;
        if (self.topics.get(topic_name)) |t| {
            topic = t;
        } else {
            topic = try self.allocator.create(ClientTopic);
            errdefer self.allocator.destroy(topic);

            topic.* = try ClientTopic.init(self.allocator, self.memory_pool, topic_name, .{});
            errdefer topic.deinit();

            try self.topics.put(topic_name, topic);
        }

        const callback_id = self.kid.generate();
        try topic.addCallback(callback_id, callback);
        errdefer _ = topic.removeCallback(callback_id);

        const subscribe_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(subscribe_message);

        subscribe_message.* = Message.new(.subscribe);
        subscribe_message.setTopicName(topic_name);
        subscribe_message.extension_headers.subscribe.transaction_id = self.kid.generate();
        subscribe_message.ref();
        errdefer subscribe_message.deref();

        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        try self.outbox.enqueue(subscribe_message);

        return callback_id;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();
}
