const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const posix = std.posix;
const log = std.log.scoped(.Worker);

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const BufferedChannel = @import("stdx").BufferedChannel;
const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;
const Envelope = @import("../data_structures/envelope.zig").Envelope;

const IO = @import("../io.zig").IO;
const Node = @import("./node.zig").Node;

const Connection = @import("../protocol/connection.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const AuthChallenge = @import("../protocol/message.zig").AuthChallenge;

const Authenticator = @import("./authenticator.zig").Authenticator;
const NoneAuthStrategy = @import("./authenticator.zig").NoneAuthStrategy;
const TokenAuthStrategy = @import("./authenticator.zig").TokenAuthStrategy;

const WorkerState = enum {
    running,
    closing,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    authenticator: *Authenticator,
    close_channel: *UnbufferedChannel(bool),
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    dead_connections_mutex: std.Thread.Mutex,
    dead_connections: std.ArrayList(u128),
    done_channel: *UnbufferedChannel(bool),
    id: usize,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(*Message),
    io: *IO,
    memory_pool: *MemoryPool(Message),
    node_id: u128,
    node: *Node,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(Envelope),
    state: WorkerState,
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),

    pub fn init(allocator: std.mem.Allocator, id: usize, node: *Node) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, node.memory_pool.capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(Envelope).init(allocator, node.memory_pool.capacity);
        errdefer outbox.deinit();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        return Self{
            .allocator = allocator,
            .authenticator = node.authenticator,
            .close_channel = close_channel,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .dead_connections_mutex = std.Thread.Mutex{},
            .dead_connections = std.ArrayList(u128).init(allocator),
            .done_channel = done_channel,
            .id = id,
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .io = io,
            .memory_pool = node.memory_pool,
            .node_id = node.id,
            .node = node,
            .outbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .state = .closed,
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        while (self.outbox.dequeue()) |envelope| {
            const message = envelope.message;
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.inbox.deinit();
        self.outbox.deinit();
        self.connections.deinit();
        self.io.deinit();
        self.uninitialized_connections.deinit();
        self.dead_connections.deinit();

        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.io);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    // self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
                },
                .closing => {
                    log.info("worker {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                else => {
                    @panic("unable to tick closed worker");
                },
            }
        }
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                while (!self.closeAllConnections()) {}
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    pub fn tick(self: *Self) !void {
        try self.tickConnections();
        try self.tickUninitializedConnections();
        try self.processInboundConnectionMessages();
        try self.processUninitializedConnectionMessages();
        try self.processOutboundConnectionMessages();
    }

    pub fn addInboundConnection(self: *Self, socket: posix.socket_t, config: InboundConnectionConfig) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        const conn_id = uuid.v7.new();
        conn.* = try Connection.init(
            conn_id,
            self.node_id,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
            .{ .inbound = config },
        );
        errdefer conn.deinit();

        // Since this is an inbound connection, we have already accepted the socket
        conn.connection_state = .connected;
        errdefer conn.protocol_state = .terminating;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, conn);
        errdefer _ = self.connections.remove(conn_id);

        conn.protocol_state = .accepting;
        const accept_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(accept_message);

        accept_message.* = Message.new2(.accept);
        accept_message.ref();
        errdefer accept_message.deref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.connection_id = conn_id;
        accept_headers.origin_id = self.node_id;

        assert(accept_message.validate() == null);

        try conn.outbox.enqueue(accept_message);

        conn.protocol_state = .authenticating;
        const challenge_method = self.authenticator.getChallengeMethod();
        const challenge_payload = self.authenticator.getChallengePayload();

        const auth_challenge_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(auth_challenge_message);

        auth_challenge_message.* = Message.new2(.auth_challenge);
        auth_challenge_message.setTransactionId(uuid.v7.new());
        auth_challenge_message.setChallengeMethod(challenge_method);
        auth_challenge_message.setBody(challenge_payload);
        auth_challenge_message.ref();
        errdefer auth_challenge_message.deref();

        assert(auth_challenge_message.validate() == null);

        try conn.outbox.enqueue(auth_challenge_message);

        log.info("worker: {} added inbound connection {}", .{ self.id, conn_id });
    }

    // TODO: the config should be passed to the connection so it can be tracked
    //     the connection needs to be able to reconnect if the config says it should
    pub fn addOutboundConnection(self: *Self, config: OutboundConnectionConfig) !void {
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
        const tmp_conn_id = uuid.v7.new();
        conn.* = try Connection.init(
            0,
            self.node_id,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
            .{ .outbound = config },
        );
        errdefer conn.deinit();

        conn.connection_state = .connecting;
        conn.protocol_state = .accepting;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.uninitialized_connections.put(tmp_conn_id, conn);
        errdefer _ = self.uninitialized_connections.remove(tmp_conn_id);

        self.io.connect(
            *Connection,
            conn,
            Connection.onConnect,
            conn.connect_completion,
            socket,
            address,
        );
        conn.connect_submitted = true;

        log.info("worker: {} added outbound connection", .{self.id});
    }

    fn removeConnection(self: *Self, conn: *Connection) void {
        _ = self.connections.remove(conn.connection_id);

        log.info("worker: {} removed connection {}", .{ self.id, conn.connection_id });
        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn cleanupUninitializedConnection(self: *Self, tmp_id: uuid.Uuid, conn: *Connection) !void {
        _ = self.uninitialized_connections.remove(tmp_id);
        log.info("worker: {} removed uninitialized_connection {}", .{ self.id, conn.connection_id });

        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        self.dead_connections_mutex.lock();
        defer self.dead_connections_mutex.unlock();

        try self.dead_connections.append(conn.connection_id);

        self.removeConnection(conn);
    }

    fn tickUninitializedConnections(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var uninitialized_connections_iter = self.uninitialized_connections.iterator();
        while (uninitialized_connections_iter.next()) |entry| {
            const tmp_id = entry.key_ptr.*;
            const conn = entry.value_ptr.*;

            switch (conn.protocol_state) {
                .terminating => {
                    log.debug("need to clean up any thing related to this connection", .{});
                    conn.protocol_state = .terminated;
                    conn.connection_state = .closing;
                },
                else => {},
            }

            // check if this connection was closed for whatever reason
            if (conn.connection_state == .closed) {
                // if this is an outbound connection???
                switch (conn.config) {
                    .outbound => |c| {
                        if (c.reconnect_config) |reconnect_config| {
                            log.info("reconnect_config {any}", .{reconnect_config});
                        }
                    },
                    else => {},
                }
                try self.cleanupUninitializedConnection(tmp_id, conn);
                break;
            }

            conn.tick() catch |err| {
                log.err("could not tick uninitialized_connection error: {any}", .{err});
                break;
            };

            if (conn.connection_state == .connected and conn.protocol_state == .ready) {
                // the connection is now valid and ready for events
                // move the connection to the regular connections map
                try self.connections.put(conn.connection_id, conn);
                // remove the connection from the uninitialized_connections map
                assert(self.uninitialized_connections.remove(tmp_id));
            }

            // // check if this connection was closed for whatever reason
            // if (conn.connection_state == .closed) {
            //     try self.cleanupUninitializedConnection(tmp_id, conn);
            //     break;
            // }

            // conn.tick() catch |err| {
            //     log.err("could not tick uninitialized_connection error: {any}", .{err});
            //     break;
            // };

            // if (conn.connection_state == .connected and conn.connection_id != 0) {
            //     // the connection is now valid and ready for events
            //     // move the connection to the regular connections map
            //     try self.connections.put(conn.connection_id, conn);
            //     errdefer _ = self.connections.remove(conn.connection_id);

            //     // remove the connection from the uninitialized_connections map
            //     assert(self.uninitialized_connections.remove(tmp_id));
            // }
        }
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

    fn checkConnRateLimit(_: Self, conn: *Connection) bool {
        const now = std.time.milliTimestamp();
        const messages_recv_rate_limit = 500;
        const messages_recv_rate_limit_interval_duration = 1_000;

        if (conn.metrics.rate_limited) {
            const current_rate_limited_duration = now - conn.metrics.rate_limited_at;

            if (current_rate_limited_duration < messages_recv_rate_limit_interval_duration) return true;

            conn.metrics.messages_recv_at_start = conn.metrics.messages_recv_total;
            conn.metrics.rate_limited = false;
            conn.metrics.rate_limited_at = 0;
            return false;
        } else {
            const messages_recv_since_interval_start = conn.metrics.messages_recv_total - conn.metrics.messages_recv_at_start;
            if (messages_recv_since_interval_start < messages_recv_rate_limit) return false;

            conn.metrics.messages_recv_at_start = conn.metrics.messages_recv_total;
            conn.metrics.rate_limited = true;
            conn.metrics.rate_limited_at = now;
            return true;
        }
    }

    fn processInboundConnectionMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |connection_entry| {
            const conn = connection_entry.*;

            if (conn.inbox.isEmpty()) continue;
            while (conn.inbox.dequeue()) |message| {
                // if this message has more than a single ref, something has not been initialized
                // or deinitialized correctly.
                assert(message.refs() == 1);

                switch (message.headers.message_type) {
                    .auth_response => try self.handleAuthResponseMessage(conn, message),
                    else => {
                        // NOTE: This message type is meant to be handled by the node
                        self.inbox_mutex.lock();
                        defer self.inbox_mutex.unlock();

                        self.inbox.enqueue(message) catch |err| {
                            try conn.inbox.enqueue(message);

                            log.err("could not enqueue envelope {any}", .{err});
                        };
                    },
                }
            }
        }
    }

    fn processUninitializedConnectionMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |connection_entry| {
            const conn = connection_entry.*;

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
                        log.err("unexpected message received from uninitialized connection", .{});
                        conn.connection_state = .closing;
                        break;
                    },
                }
            }
        }
    }

    fn processOutboundConnectionMessages(self: *Self) !void {
        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        while (self.outbox.dequeue()) |envelope| {
            const message = envelope.message;
            if (self.connections.get(envelope.connection_id)) |connection| {
                // FIX: there should be a handler for this issue
                connection.outbox.enqueue(message) catch {
                    self.outbox.prepend(envelope) catch unreachable;
                    break;
                };
            } else {
                // The connection has disappeared since and this should be destroyed
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }
        }
    }

    // FIX: the `node` should be the one to validate this message.
    //  We need to ensure that this conn.connection_id matches the message.origin_id
    fn handleAuthResponseMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        log.debug("received auth_response from origin_id: {}, connection_id: {}", .{
            message.headers.origin_id,
            message.headers.connection_id,
        });

        const auth_result = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(auth_result);

        auth_result.* = Message.new2(.auth_result);
        auth_result.setTransactionId(message.transactionId());
        auth_result.setErrorCode(.ok);
        auth_result.ref();
        errdefer auth_result.deref();

        if (conn.connection_id != message.headers.connection_id) {
            log.err("unexpected connection_id on authentication", .{});
            auth_result.setErrorCode(.err);
        }

        const authenticator = self.authenticator;

        switch (authenticator.strategy_type) {
            .none => {
                const ctx: NoneAuthStrategy.Context = .{};
                if (!authenticator.authenticate(&ctx)) auth_result.setErrorCode(.unauthorized);
            },
            .token => {
                const ctx: TokenAuthStrategy.Context = .{ .token = message.body() };
                if (!authenticator.authenticate(&ctx)) auth_result.setErrorCode(.unauthorized);
            },
        }

        if (auth_result.errorCode() != .ok) conn.protocol_state = .terminating;

        try conn.outbox.enqueue(auth_result);
        log.info("sending auth_result", .{});
    }

    fn handleAcceptMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        // ensure that this connection is fully connected
        assert(conn.connection_state == .connected);
        // ensure the client.connection is expecting this accept message
        assert(conn.protocol_state == .accepting);

        log.info("accept message received!", .{});
        switch (conn.config) {
            .outbound => {
                assert(conn.connection_id == 0);
                assert(conn.peer_id != message.headers.origin_id);
                assert(conn.connection_id != message.headers.connection_id);

                conn.connection_id = message.headers.connection_id;
                conn.peer_id = message.headers.origin_id;

                conn.connection_state = .connected;
                conn.protocol_state = .authenticating;

                log.info("outbound_connection - origin_id: {}, connection_id: {}, remote_id: {}, peer_type: {any}", .{
                    conn.origin_id,
                    conn.connection_id,
                    conn.peer_id,
                    conn.config.outbound.peer_type,
                });
            },
            .inbound => unreachable,
        }
    }

    fn handleAuthChallengeMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        // ensure that this connection is fully connected
        assert(conn.connection_state == .connected);
        // ensure the client.connection is expecting this challenge message
        assert(conn.protocol_state == .authenticating);

        const auth_challenge_message: *const AuthChallenge = message.headers.intoConst(.auth_challenge).?;

        const auth_response_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(auth_response_message);

        auth_response_message.* = Message.new2(.auth_response);
        auth_response_message.setTransactionId(message.transactionId());
        auth_response_message.setChallengeMethod(auth_challenge_message.challenge_method);
        auth_response_message.ref();
        errdefer auth_response_message.deref();

        switch (auth_challenge_message.challenge_method) {
            .none => auth_response_message.setBody(""),
            .token => {
                switch (conn.config) {
                    .outbound => |c| {
                        if (c.authentication_config) |auth_config| {
                            if (auth_config.token_config) |token_config| {
                                auth_response_message.setBody(token_config.token);
                            }
                        }
                    },
                    .inbound => unreachable,
                }
            },
        }

        assert(auth_response_message.validate() == null);

        try conn.outbox.enqueue(auth_response_message);

        log.debug("sending auth_response", .{});
    }

    // FIX: this doesn't actually fulfill a transaction or enforce that THIS connection is the one authenticating
    //   There should be a mechanism where we lookup and clear a transaction
    pub fn handleAuthResultMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        assert(conn.connection_state == .connected);
        assert(conn.protocol_state == .authenticating);

        if (message.errorCode() != .ok) {
            log.err("connection did not successfully authenticate. {any}", .{message.errorCode()});
            conn.protocol_state = .terminating;
            return;
        }

        log.info("connection successfully authenticated", .{});

        conn.protocol_state = .ready;
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
                    all_connections_closed = false;
                },
            }

            conn.tick() catch |err| {
                log.err("worker uninitialized_connection tick err {any}", .{err});
                unreachable;
            };
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
                    all_connections_closed = false;
                },
            }

            conn.tick() catch |err| {
                log.err("worker connection tick err {any}", .{err});
                unreachable;
            };
        }

        return all_connections_closed;
    }
};
