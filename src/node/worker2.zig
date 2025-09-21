const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.Worker);
const posix = std.posix;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const uuid = @import("uuid");

const KID = @import("kid").KID;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const RingBuffer = @import("stdx").RingBuffer;

const IO = @import("../io.zig").IO;

const Connection = @import("../protocol/connection2.zig").Connection;
const InboundConnectionConfig = @import("../protocol/connection2.zig").InboundConnectionConfig;
const Envelope = @import("./envelope.zig").Envelope;
const Node = @import("./node.zig").Node;

const Message = @import("../protocol/message2.zig").Message;
const ChallengeMethod = @import("../protocol/message2.zig").ChallengeMethod;
const ChallengeAlgorithm = @import("../protocol/message2.zig").ChallengeAlgorithm;
const TokenEntry = @import("./authenticator.zig").TokenAuthStrategy.TokenEntry;
const Session = @import("./session.zig").Session;

const Handshake = struct {
    nonce: u128,
    connection_id: u64,
    challenge_method: ChallengeMethod,
    algorithm: ChallengeAlgorithm,
};

const WorkerState = enum {
    running,
    closing,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(u64, *Connection),
    // dead_connections_mutex: std.Thread.Mutex,
    // dead_connections: std.array_list.Managed(u128),
    done_channel: *UnbufferedChannel(bool),
    id: usize,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(Envelope),
    io: *IO,
    // node_id: u128,
    node: *Node,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(Envelope),
    state: WorkerState,
    conn_session_map: std.AutoHashMapUnmanaged(u64, u64),
    handshakes: std.AutoHashMapUnmanaged(u64, Handshake),
    // uninitialized_connections: std.AutoHashMap(u64, *Connection),

    pub fn init(
        allocator: std.mem.Allocator,
        node: *Node,
        id: usize,
    ) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const inbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(Envelope).init(allocator, 1_000);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(Envelope).init(allocator, 1_000);
        errdefer outbox.deinit();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        return Self{
            .allocator = allocator,
            .close_channel = close_channel,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(u64, *Connection).init(allocator),
            .handshakes = .empty,
            .conn_session_map = .empty,
            .done_channel = done_channel,
            .id = id,
            .inbox = inbox,
            .inbox_mutex = .{},
            .node = node,
            .io = io,
            .outbox_mutex = .{},
            .outbox = outbox,
            .state = .closed,
        };
    }

    pub fn deinit(self: *Self) void {
        while (self.inbox.dequeue()) |envelope| {
            envelope.message.deref();
            if (envelope.message.refs() == 0) self.node.memory_pool.destroy(envelope.message);
        }

        while (self.outbox.dequeue()) |envelope| {
            envelope.message.deref();
            if (envelope.message.refs() == 0) self.node.memory_pool.destroy(envelope.message);
        }

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.*;

            conn.deinit();
            self.allocator.destroy(conn);
        }

        self.inbox.deinit();
        self.outbox.deinit();
        self.io.deinit();
        self.connections.deinit();
        self.handshakes.deinit(self.allocator);
        self.conn_session_map.deinit(self.allocator);

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.outbox);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {d}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {d} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                },
                .closing => {
                    log.info("worker {d}: closed", .{self.id});
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

    fn closeAllConnections(self: *Self) bool {
        var all_connections_closed = true;

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

    pub fn tick(self: *Self) !void {
        try self.tickConnections();
        // try self.tickUninitializedConnections();
        try self.processInboundConnectionMessages();
        // try self.processUninitializedConnectionMessages();
        // try self.processOutboundConnectionMessages();

    }

    pub fn tickConnections(self: *Self) !void {
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

    pub fn processInboundConnectionMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;

            while (conn.inbox.dequeue()) |message| {
                switch (message.fixed_headers.message_type) {
                    .session_init => try self.handleSessionInit(conn, message),
                    .session_join => try self.handleSessionJoin(conn, message),
                    // TODO: .session_join

                    else => unreachable,
                }
            }
        }
    }

    pub fn addInboundConnection(self: *Self, socket: posix.socket_t, config: InboundConnectionConfig) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        const conn_id = self.node.kid.generate();
        conn.* = try Connection.init(
            conn_id,
            self.io,
            socket,
            self.allocator,
            self.node.memory_pool,
            .{ .inbound = config },
        );
        errdefer conn.deinit();

        conn.connection_state = .connected;
        errdefer conn.protocol_state = .terminating;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, conn);
        errdefer _ = self.connections.remove(conn_id);

        const auth_challenge = try self.node.memory_pool.create();
        errdefer self.node.memory_pool.destroy(auth_challenge);

        auth_challenge.* = Message.new(0, .auth_challenge);
        auth_challenge.ref();
        errdefer auth_challenge.deref();

        auth_challenge.extension_headers.auth_challenge.challenge_method = .token;
        auth_challenge.extension_headers.auth_challenge.nonce = uuid.v7.new();
        auth_challenge.extension_headers.auth_challenge.connection_id = conn_id;

        conn.protocol_state = .authenticating;

        assert(auth_challenge.validate() == null);

        const handshake = Handshake{
            .nonce = auth_challenge.extension_headers.auth_challenge.nonce,
            .connection_id = conn_id,
            .challenge_method = auth_challenge.extension_headers.auth_challenge.challenge_method,
            .algorithm = auth_challenge.extension_headers.auth_challenge.algorithm,
        };

        try self.handshakes.put(self.allocator, conn.connection_id, handshake);
        errdefer _ = self.handshakes.remove(conn.connection_id);

        try conn.outbox.enqueue(auth_challenge);

        log.info("worker: {d} added inbound connection {d}", .{ self.id, conn_id });
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        const conn_id = conn.connection_id;
        defer log.info("worker: {} removed connection {}", .{ self.id, conn_id });

        if (self.conn_session_map.fetchRemove(conn.connection_id)) |kv_entry| {
            const session_id = kv_entry.value;

            _ = self.node.removeConnectionFromSession(session_id, conn_id);
        }

        _ = self.connections.remove(conn.connection_id);

        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn handleSessionInit(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }
        // Ensure only one handshake per connection
        const entry = self.handshakes.fetchRemove(conn.connection_id) orelse return error.HandshakeMissing;

        const handshake = entry.value;

        const reply = try self.node.memory_pool.create();
        errdefer self.node.memory_pool.destroy(reply);

        switch (handshake.challenge_method) {
            .token => {
                if (self.authenticate(handshake, message)) {
                    const session_init = message.extension_headers.session_init;

                    const session = try self.node.createSession(session_init.peer_id, session_init.peer_type);
                    errdefer self.node.removeSession(session.session_id);

                    try self.node.addConnectionToSession(session.session_id, conn);
                    errdefer _ = self.node.removeConnectionFromSession(session.session_id, conn.connection_id);

                    reply.* = Message.new(0, .auth_success);
                    reply.ref();
                    errdefer reply.deref();

                    reply.extension_headers.auth_success.peer_id = session.peer_id;
                    reply.extension_headers.auth_success.session_id = session.session_id;
                    reply.setBody(session.session_token);

                    try self.conn_session_map.put(self.allocator, conn.connection_id, session.session_id);
                    errdefer _ = self.conn_session_map.remove(conn.connection_id);

                    try conn.outbox.enqueue(reply);
                } else {
                    reply.* = Message.new(0, .auth_failure);
                    reply.ref();
                    errdefer reply.deref();

                    reply.extension_headers.auth_failure.error_code = .unauthorized;
                    try conn.outbox.enqueue(reply);
                }
            },
            else => @panic("unsupported challenge_method"),
        }
    }

    fn handleSessionJoin(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }

        // Ensure only one handshake per connection
        const handshake_entry = self.handshakes.fetchRemove(conn.connection_id) orelse return error.HandshakeMissing;
        const handshake = handshake_entry.value;

        const reply = try self.node.memory_pool.create();
        errdefer self.node.memory_pool.destroy(reply);

        switch (handshake.challenge_method) {
            .token => {
                if (self.authenticateWithSession(handshake, message)) {
                    log.info("successfully authenticated!", .{});
                    const session_join_headers = message.extension_headers.session_join;

                    try self.node.addConnectionToSession(session_join_headers.session_id, conn);
                    errdefer _ = self.node.removeConnectionFromSession(session_join_headers.session_id, conn.connection_id);

                    reply.* = Message.new(0, .auth_success);
                    reply.ref();
                    errdefer reply.deref();

                    reply.extension_headers.auth_success.peer_id = session_join_headers.peer_id;
                    reply.extension_headers.auth_success.session_id = session_join_headers.session_id;

                    try self.conn_session_map.put(self.allocator, conn.connection_id, session_join_headers.session_id);
                    errdefer _ = self.conn_session_map.remove(conn.connection_id);

                    try conn.outbox.enqueue(reply);
                } else {
                    log.info("authentication unsuccessful", .{});
                    reply.* = Message.new(0, .auth_failure);
                    reply.ref();
                    errdefer reply.deref();

                    reply.extension_headers.auth_failure.error_code = .unauthorized;
                    try conn.outbox.enqueue(reply);
                }
            },
            else => @panic("unsupported challenge_method"),
        }
    }

    fn authenticate(self: *Self, handshake: Handshake, message: *Message) bool {
        const auth_token_config = self.node.config.authenticator_config.token;

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
            .node => @panic("unsupported peer type"),
        }

        return false;
    }

    fn authenticateWithSession(self: *Self, handshake: Handshake, message: *Message) bool {
        const session_opt = self.node.getSession(message.extension_headers.session_join.session_id);
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
};

test "init/deinit" {
    const allocator = testing.allocator;

    var node = try Node.init(allocator, .{});
    defer node.deinit();

    var worker = try Worker.init(allocator, &node, 0);
    defer worker.deinit();
}

test "ticking the connection" {
    const allocator = testing.allocator;

    var node = try Node.init(allocator, .{});
    defer node.deinit();

    var worker = try Worker.init(allocator, &node, 0);
    defer worker.deinit();
}
