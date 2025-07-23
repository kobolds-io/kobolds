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
const Envelope = @import("../data_structures/envelope.zig").Envelope;

const IO = @import("../io.zig").IO;
const Node = @import("./node.zig").Node;

const Connection = @import("../protocol/connection.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;

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
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    dead_connections_mutex: std.Thread.Mutex,
    dead_connections: std.ArrayList(u128),
    done_channel: *UnbufferedChannel(bool),
    id: usize,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(*Message),
    io: *IO,
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
            .close_channel = close_channel,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .done_channel = done_channel,
            .id = id,
            .io = io,
            .node = node,
            .state = .closed,
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .outbox_mutex = std.Thread.Mutex{},
            .dead_connections = std.ArrayList(u128).init(allocator),
            .dead_connections_mutex = std.Thread.Mutex{},
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
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }

        while (self.outbox.dequeue()) |envelope| {
            const message = envelope.message;
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
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
                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                    // self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
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
        // try self.processUninitializedConnectionMessages();
        try self.processOutboundConnectionMessages();
    }

    pub fn addInboundConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const default_inbound_connection_config = InboundConnectionConfig{};

        const conn_id = uuid.v7.new();
        connection.* = try Connection.init(
            conn_id,
            self.node.id,
            .inbound,
            self.io,
            socket,
            self.allocator,
            self.node.memory_pool,
            .{ .inbound = default_inbound_connection_config },
        );
        errdefer connection.deinit();

        connection.state = .connecting;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, connection);
        errdefer _ = self.connections.remove(conn_id);

        const accept_message = self.node.memory_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .closing;
            return;
        };
        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.connection_id = conn_id;
        accept_headers.origin_id = self.node.id;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.id, conn_id });
    }

    // TODO: the config should be passed to the connection so it can be tracked
    //     the connection needs to be able to reconnect if the config says it should
    pub fn addOutboundConnection(self: *Self, config: OutboundConnectionConfig) !*Connection {
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
            self.node.id,
            .outbound,
            self.io,
            socket,
            self.allocator,
            self.node.memory_pool,
            .{ .outbound = config },
        );
        errdefer conn.deinit();

        conn.state = .connecting;

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

        return conn;
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

            // check if this connection was closed for whatever reason
            if (conn.state == .closed) {
                try self.cleanupUninitializedConnection(tmp_id, conn);
                break;
            }

            conn.tick() catch |err| {
                log.err("could not tick uninitialized_connection error: {any}", .{err});
                break;
            };

            // try self.process(conn);

            if (conn.state == .connected and conn.connection_id != 0) {
                // the connection is now valid and ready for events
                // move the connection to the regular connections map
                try self.connections.put(conn.connection_id, conn);
                errdefer _ = self.connections.remove(conn.connection_id);

                // remove the connection from the uninitialized_connections map
                assert(self.uninitialized_connections.remove(tmp_id));
            }
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
            if (conn.state == .closed) {
                try self.cleanupConnection(conn);
                continue;
            }

            conn.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };
        }
    }

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

                switch (message.headers.message_type) {
                    .accept => try self.handleAcceptMessage(conn, message),
                    .ping => try self.handlePingMessage(conn, message),
                    .pong => try self.handlePongMessage(conn, message),
                    // .subscribe => try self.handleSubscribeMessage(conn, message),
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
                    else => {
                        log.err("unexpected message received from uninitialized connection", .{});
                        conn.state = .closing;
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
                try connection.outbox.enqueue(message);
            } else {
                // The connection has disappeared since and this should be destroyed
                message.deref();
                if (message.refs() == 0) self.node.memory_pool.destroy(message);
            }
        }
    }

    fn handleAcceptMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }

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

                assert(conn.connection_type == .outbound);

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
    }

    fn handlePingMessage(self: *Self, conn: *Connection, message: *Message) !void {
        log.debug("received ping from origin_id: {}, connection_id: {}", .{
            message.headers.origin_id,
            message.headers.connection_id,
        });
        // Since this is a `ping` we don't need to do any extra work to figure out how to respond
        message.headers.message_type = .pong;
        message.headers.origin_id = self.node.id;
        message.headers.connection_id = conn.connection_id;
        message.setTransactionId(message.transactionId());
        message.setErrorCode(.ok);

        assert(message.refs() == 1);

        if (conn.outbox.enqueue(message)) |_| {} else |err| {
            log.err("Failed to enqueue message to outbox: {}", .{err});
            message.deref(); // Undo reference if enqueue fails
        }
    }

    fn handlePongMessage(self: *Self, _: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }

        log.debug("received pong from origin_id: {}, connection_id: {}", .{
            message.headers.origin_id,
            message.headers.connection_id,
        });
    }

    fn closeAllConnections(self: *Self) bool {
        var all_connections_closed = true;

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |entry| {
            var conn = entry.*;
            switch (conn.state) {
                .closed => continue,
                .closing => {
                    all_connections_closed = false;
                },
                else => {
                    conn.state = .closing;
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
            switch (conn.state) {
                .closed => continue,
                .closing => {
                    all_connections_closed = false;
                },
                else => {
                    conn.state = .closing;
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
