const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const IO = @import("../io.zig").IO;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const Signal = @import("stdx").Signal;
const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

// const Accept = @import("../protocol/message.zig").Accept;
// const AuthChallenge = @import("../protocol/message2.zig").AuthChallenge;
const Message = @import("../protocol/message2.zig").Message;
const Connection = @import("../protocol/connection2.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;

const Topic = @import("../pubsub/topic.zig").Topic;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const SubscriberCallback = *const fn (message: *Message) void;

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
    max_connections: u16 = 100,
    memory_pool_capacity: usize = 10_000,
    authentication_config: AuthenticationConfig = .{},
};

const ClientState = enum {
    running,
    closing,
    closed,
};

pub const Client = struct {
    const Self = @This();

    id: uuid.Uuid,
    allocator: std.mem.Allocator,
    config: ClientConfig,
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    io: *IO,
    state: ClientState,
    memory_pool: *MemoryPool(Message),
    mutex: std.Thread.Mutex,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(u64, *Connection),
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),
    transactions: std.AutoHashMap(uuid.Uuid, *Signal(*Message)),
    transactions_mutex: std.Thread.Mutex,
    connection_messages: ConnectionMessages,
    connection_messages_mutex: std.Thread.Mutex,
    topics: std.StringHashMap(*Topic),
    topics_mutex: std.Thread.Mutex,
    subscriber_callbacks: std.AutoHashMap(u128, SubscriberCallback),
    services: std.StringHashMap(*Service),
    services_mutex: std.Thread.Mutex,
    advertiser_callbacks: std.AutoHashMap(u128, AdvertiserCallback),
    inbox: *RingBuffer(*Message),
    inbox_mutex: std.Thread.Mutex,
    session: ?*Session = null,

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

        memory_pool.* = try MemoryPool(Message).init(allocator, 10_000);
        errdefer memory_pool.deinit();

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, 5_000);
        errdefer inbox.deinit();

        // TODO: we should add an outbox to act as the global outbound communicator

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            .done_channel = done_channel,
            .io = io,
            .memory_pool = memory_pool,
            .state = .closed,
            .mutex = std.Thread.Mutex{},
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(u64, *Connection).init(allocator),
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .transactions = std.AutoHashMap(uuid.Uuid, *Signal(*Message)).init(allocator),
            .transactions_mutex = std.Thread.Mutex{},
            .connection_messages = ConnectionMessages.init(allocator, memory_pool),
            .connection_messages_mutex = std.Thread.Mutex{},
            .topics = std.StringHashMap(*Topic).init(allocator),
            .topics_mutex = std.Thread.Mutex{},
            .subscriber_callbacks = std.AutoHashMap(u128, SubscriberCallback).init(allocator),
            .services = std.StringHashMap(*Service).init(allocator),
            .services_mutex = std.Thread.Mutex{},
            .advertiser_callbacks = std.AutoHashMap(u128, AdvertiserCallback).init(allocator),
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .session = null,
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
        self.subscriber_callbacks.deinit();
        self.services.deinit();
        self.advertiser_callbacks.deinit();
        self.inbox.deinit();

        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.inbox);
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

    pub fn connect2(self: *Self, config: OutboundConnectionConfig, timeout_ns: i128) !void {
        _ = self;
        _ = timeout_ns;

        if (config.validate()) |error_reason| {
            log.err("invalid config: {s}", .{error_reason});
            return error.InvalidConfig;
        }
    }

    pub fn connect(self: *Self, config: OutboundConnectionConfig, timeout_ns: i128) !*Connection {
        if (config.validate()) |msg| {
            log.err("{s}", .{msg});
            return error.InvalidConfig;
        }

        const conn = try self.addOutboundConnection(config);
        errdefer self.disconnect(conn);

        var timer = try std.time.Timer.start();
        defer timer.reset();
        const timer_start = timer.read();

        const deadline = std.time.nanoTimestamp() + timeout_ns;
        while (deadline > std.time.nanoTimestamp()) {
            // log.debug("conn.connection_state {any}, conn.protocol_state {any}", .{
            //     conn.connection_state,
            //     conn.protocol_state,
            // });

            if (conn.connection_state == .connected and conn.protocol_state == .ready) break;

            // // FIX: this is some baaaaad code. There should instead be a signal or channel that this thread could
            // // wait on instead. Since this call happens on a foreground thread, an unbuffered channel seems the most
            // // appropriate.
            std.Thread.sleep(constants.io_tick_us * std.time.ns_per_us);
        } else {
            return error.DeadlineExceeded;
        }

        const timer_end = timer.read();
        log.info("connected and ready in {}us", .{(timer_end - timer_start) / std.time.ns_per_us});

        return conn;
    }

    pub fn disconnect(self: *Self, conn: *Connection) void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        conn.connection_state = .closing;
    }

    pub fn awaitConnected(self: *Self, conn: *Connection, timeout_ns: i128) !void {
        _ = self;
        const deadline = std.time.nanoTimestamp() + timeout_ns;

        while (deadline > std.time.nanoTimestamp()) {
            _ = conn;
        }
    }

    fn tick(self: *Self) !void {
        try self.tickConnections();
        // try self.tickUninitializedConnections();
        try self.processInboundConnectionMessages();
        // try self.processUninitializedConnectionMessages();
        // try self.processClientMessages();
        // try self.aggregateOutboundMessages();
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

    // fn tickUninitializedConnections(self: *Self) !void {
    //     self.connections_mutex.lock();
    //     defer self.connections_mutex.unlock();

    //     var uninitialized_connections_iter = self.uninitialized_connections.iterator();
    //     while (uninitialized_connections_iter.next()) |entry| {
    //         const tmp_id = entry.key_ptr.*;
    //         const conn = entry.value_ptr.*;

    //         switch (conn.protocol_state) {
    //             .terminating => {
    //                 log.debug("need to clean up any thing related to this connection", .{});
    //                 conn.protocol_state = .terminated;
    //                 conn.connection_state = .closing;
    //             },
    //             else => {},
    //         }

    //         // check if this connection was closed for whatever reason
    //         if (conn.connection_state == .closed) {
    //             try self.cleanupUninitializedConnection(tmp_id, conn);
    //             break;
    //         }

    //         conn.tick() catch |err| {
    //             log.err("could not tick uninitialized_connection error: {any}", .{err});
    //             break;
    //         };

    //         if (conn.connection_state == .connected and conn.protocol_state == .ready) {
    //             // the connection is now valid and ready for events
    //             // move the connection to the regular connections map
    //             try self.connections.put(conn.connection_id, conn);
    //             // remove the connection from the uninitialized_connections map
    //             assert(self.uninitialized_connections.remove(tmp_id));
    //         }
    //     }
    // }

    fn processInboundConnectionMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |connection_entry| {
            const conn = connection_entry.*;

            if (conn.inbox.count == 0) continue;
            while (conn.inbox.dequeue()) |message| {
                // if this message has more than a single ref, something has not been initialized
                // or deinitialized correctly.
                assert(message.refs() == 1);

                switch (message.fixed_headers.message_type) {
                    .auth_challenge => try self.handleAuthChallengeMessage(conn, message),
                    .auth_success => try self.handleAuthSuccessMessage(conn, message),
                    else => unreachable,
                }
            }
        }
    }

    fn processUninitializedConnectionMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |uninitialized_connection_entry| {
            const conn = uninitialized_connection_entry.*;

            if (conn.inbox.count == 0) continue;
            while (conn.inbox.dequeue()) |message| {
                // if this message has more than a single ref, something has not been initialized
                // or deinitialized correctly.
                assert(message.refs() == 1);

                switch (message.headers.message_type) {
                    .accept => try self.handleAcceptMessage(conn, message),
                    .auth_challenge => try self.handleAuthChallengeMessage(conn, message),
                    .auth_result => try self.handleAuthResultMessage(conn, message),
                    else => {
                        log.err("received unexpected message {any}", .{message.headers.message_type});
                        message.deref();
                        if (message.refs() == 0) self.memory_pool.destroy(message);
                    },
                }
            }
        }
    }

    fn processClientMessages(self: *Self) !void {
        {
            self.inbox_mutex.lock();
            defer self.inbox_mutex.unlock();

            while (self.inbox.dequeue()) |message| {
                switch (message.headers.message_type) {
                    .reply => try self.handleReplyMessage(message),
                    .request => try self.handleRequestMessage(message),
                    .publish => try self.handlePublishMessage(message),
                    else => unreachable,
                }
            }
        }

        {
            self.topics_mutex.lock();
            defer self.topics_mutex.unlock();

            var topics_iter = self.topics.valueIterator();
            while (topics_iter.next()) |topic_entry| {
                const topic = topic_entry.*;

                try topic.tick();

                var subscribers_iter = topic.subscribers.valueIterator();
                while (subscribers_iter.next()) |subscriber_entry| {
                    const subscriber: *Subscriber = subscriber_entry.*;
                    if (subscriber.queue.count == 0) continue;

                    // get the callback for this subscriber and process all the messages
                    const cb = self.subscriber_callbacks.get(subscriber.key).?;
                    while (subscriber.queue.dequeue()) |message| cb(message);
                }
            }
        }

        {
            self.services_mutex.lock();
            defer self.services_mutex.unlock();

            var services_iter = self.services.valueIterator();
            while (services_iter.next()) |service_entry| {
                const service = service_entry.*;

                try service.tick();

                var advertisers_iter = service.advertisers.valueIterator();
                while (advertisers_iter.next()) |advertiser_entry| {
                    const advertiser: *Advertiser = advertiser_entry.*;
                    if (advertiser.queue.count == 0) continue;

                    // get the callback for this advertiser and process all the messages
                    const cb = self.advertiser_callbacks.get(advertiser.key).?;
                    while (advertiser.queue.dequeue()) |req| {
                        const rep = try self.memory_pool.create();
                        errdefer self.memory_pool.destroy(rep);

                        rep.* = Message.new();
                        rep.headers.message_type = .reply;
                        rep.setTopicName(req.topicName());
                        rep.setTransactionId(req.transactionId());
                        rep.setErrorCode(.ok);
                        rep.ref();
                        errdefer rep.deref();

                        cb(req, rep);

                        try service.replies_queue.enqueue(rep);
                    }
                }

                var requestors_iter = service.requestors.valueIterator();
                if (requestors_iter.next()) |requestor_entry| {
                    const requestor: *Requestor = requestor_entry.*;
                    if (requestor.queue.count == 0) continue;

                    self.connection_messages_mutex.lock();
                    defer self.connection_messages_mutex.unlock();

                    if (self.connection_messages.map.get(requestor.conn_id)) |connection_messages| {
                        connection_messages.concatenateAvailable(requestor.queue);
                    } else unreachable;
                }
            }
        }
    }

    fn aggregateOutboundMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        self.connection_messages_mutex.lock();
        defer self.connection_messages_mutex.unlock();

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |connection_entry| {
            const conn = connection_entry.*;

            if (self.connection_messages.map.get(conn.connection_id)) |messages| {
                conn.outbox.concatenateAvailable(messages);
            }
        }
    }

    fn handleAuthSuccessMessage(self: *Self, conn: *Connection, message: *Message) !void {
        assert(self.session == null);
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

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

        // log.info("successfully authenticated peer_id: {}, conn_id: {}, session_id: {}, session_token: {any}", .{
        //     peer_id,
        //     conn.connection_id,
        //     session_id,
        //     message.body(),
        // });
    }

    fn handleAuthChallengeMessage(self: *Self, conn: *Connection, message: *Message) !void {
        // we should totally crash because this is a sequencing issue
        assert(conn.protocol_state == .authenticating);

        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        const tmp_conn_id = conn.connection_id;
        // we need to swap the ID of this connection
        defer _ = self.connections.remove(tmp_conn_id);

        // we immediately assign this connection the id it receives from the node
        conn.connection_id = message.extension_headers.auth_challenge.connection_id;

        try self.connections.put(conn.connection_id, conn);
        errdefer _ = self.connections.remove(conn.connection_id);

        const session_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(session_message);

        if (self.session) |session| {
            _ = session;
            @panic("session already exists!!!!! can't join yet");
        } else {
            session_message.* = Message.new(0, .session_init);
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

                                std.mem.writeInt(u128, &nonce_slice, message.extension_headers.auth_challenge.nonce, .big);

                                // convert the nonce to a slice
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

    fn cleanupUninitializedConnection(self: *Self, tmp_id: uuid.Uuid, conn: *Connection) !void {
        log.debug("remove uninitialized connection called", .{});

        _ = self.uninitialized_connections.remove(tmp_id);
        log.info("client: {} removed uninitialized_connection {}", .{ self.id, conn.connection_id });

        conn.deinit();
        self.allocator.destroy(conn);
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

    fn addOutboundConnection(self: *Self, config: OutboundConnectionConfig) !*Connection {
        // create the socket
        const address = try std.net.Address.parseIp4(config.host, config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const socket = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        // create a temporary id that will be used to identify this connection until it receives a proper
        // connection_id from the remote node
        const tmp_conn_id = 1;
        conn.* = try Connection.init(
            tmp_conn_id,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
            .{ .outbound = config },
        );
        errdefer conn.deinit();

        conn.connection_state = .connecting;
        conn.protocol_state = .authenticating;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

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

        return conn;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.start();
    defer client.close();
}
