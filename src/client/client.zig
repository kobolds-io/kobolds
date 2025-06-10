const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");

// const Worker = @import("./worker.zig").Worker;
// const Listener = @import("./listener.zig").Listener;
// const ListenerConfig = @import("./listener.zig").ListenerConfig;
// const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const IO = @import("../io.zig").IO;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;

const PingOptions = struct {};

pub const ClientConfig = struct {
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 5_000,
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
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),
    transactions: std.AutoHashMap(uuid.Uuid, *UnbufferedChannel(*Message)),
    transactions_mutex: std.Thread.Mutex,
    connection_messages: ConnectionMessages,
    connection_messages_mutex: std.Thread.Mutex,

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
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .transactions = std.AutoHashMap(uuid.Uuid, *UnbufferedChannel(*Message)).init(allocator),
            .transactions_mutex = std.Thread.Mutex{},
            .connection_messages = ConnectionMessages.init(allocator),
            .connection_messages_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iterator = self.connections.valueIterator();
        while (connections_iterator.next()) |entry| {
            const connection = entry.*;

            assert(connection.state == .closed);

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var uninitialized_connections_iterator = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iterator.next()) |entry| {
            const connection = entry.*;

            assert(connection.state == .closed);

            connection.deinit();
            self.allocator.destroy(connection);
        }

        // var transactions_iterator = self.transactions.valueIterator();
        // while (transactions_iterator.next()) |entry| {
        // const transaction = entry.*;

        // self.allocator.destroy(transaction);
        // }

        self.io.deinit();
        self.memory_pool.deinit();
        self.connections.deinit();
        self.uninitialized_connections.deinit();
        self.transactions.deinit();
        self.connection_messages.deinit();

        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.close_channel);
    }

    pub fn start(self: *Self) !void {
        // Start the client thread
        var ready_channel = UnbufferedChannel(bool).new();
        const client_thread = try std.Thread.spawn(.{}, Client.run, .{ self, &ready_channel });
        client_thread.detach();

        _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
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
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("client {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;

                    // self.io.run_for_ns(100 * std.time.ns_per_us) catch |err| {
                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
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

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                self.connections_mutex.lock();
                defer self.connections_mutex.unlock();

                while (!self.closeAllConnections()) {}

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
    }

    pub fn connect(self: *Self, config: OutboundConnectionConfig, timeout_ns: i128) !*Connection {
        if (config.validate()) |msg| {
            log.err("{s}", .{msg});
            return error.InvalidConfig;
        }

        const conn = try self.addOutboundConnection(config);
        errdefer self.disconnect(conn);

        const deadline = std.time.nanoTimestamp() + timeout_ns;
        while (deadline > std.time.nanoTimestamp()) {
            if (conn.state == .connected) return conn;

            std.time.sleep(constants.io_tick_ms * std.time.ns_per_ms);
        } else {
            return error.DeadlineExceeded;
        }
    }

    pub fn disconnect(self: *Self, conn: *Connection) void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        conn.state = .closing;
    }

    pub fn awaitConnected(self: *Self, conn: *Connection, timeout_ns: i128) !void {
        _ = self;
        const deadline = std.time.nanoTimestamp() + timeout_ns;

        while (deadline > std.time.nanoTimestamp()) {
            _ = conn;
        }
    }

    fn tick(self: *Self) !void {
        {
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

                try self.gather(conn);

                if (conn.state == .connected and conn.connection_id != 0) {
                    // the connection is now valid and ready for events
                    // move the connection to the regular connections map
                    try self.connections.put(conn.connection_id, conn);
                    // remove the connection from the uninitialized_connections map
                    assert(self.uninitialized_connections.remove(tmp_id));
                }
            }

            // loop over all connections and gather their messages
            var connections_iter = self.connections.iterator();
            while (connections_iter.next()) |entry| {
                const conn = entry.value_ptr.*;

                // check if this connection was closed for whatever reason
                if (conn.state == .closed) {
                    // try self.cleanupConnection(conn);
                    continue;
                }

                conn.tick() catch |err| {
                    log.err("could not tick connection error: {any}", .{err});
                    continue;
                };

                try self.gather(conn);
                try self.distribute(conn);
            }
        }
    }

    fn gather(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        while (conn.inbox.dequeue()) |message| {
            // defer self.node.processed_messages_count += 1;
            defer {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }
            switch (message.headers.message_type) {
                .accept => {
                    // ensure that this connection is not fully connected
                    assert(conn.state != .connected);

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
                // .ping => {
                //     log.debug("received ping from origin_id: {}, connection_id: {}", .{
                //         message.headers.origin_id,
                //         message.headers.connection_id,
                //     });
                //     // Since this is a `ping` we don't need to do any extra work to figure out how to respond
                //     message.headers.message_type = .pong;
                //     message.headers.origin_id = self.id;
                //     message.headers.connection_id = conn.connection_id;
                //     message.setTransactionId(message.transactionId());
                //     message.setErrorCode(.ok);

                //     assert(message.refs() == 1);

                //     if (conn.outbox.enqueue(message)) |_| {} else |err| {
                //         log.err("Failed to enqueue message to outbox: {}", .{err});
                //         message.deref(); // Undo reference if enqueue fails
                //     }
                //     message.ref();
                // },
                .pong => {
                    // log.debug("received pong! {any}", .{message});

                    self.transactions_mutex.lock();
                    defer self.transactions_mutex.unlock();

                    // check if this pong message is part of transaction
                    if (self.transactions.get(message.transactionId())) |channel| {
                        message.ref();
                        // log.info("message.refs() {}", .{message.refs()});
                        channel.send(message);
                        _ = self.transactions.remove(message.transactionId());
                    }

                    // log.debug("received pong from origin_id: {}, connection_id: {}", .{
                    //     message.headers.origin_id,
                    //     message.headers.connection_id,
                    // });
                },
                .publish => {
                    // // get the publisher's key
                    // const publisher_key = utils.generateKey(message.topicName(), conn.connection_id);
                    // if (self.publishers.get(publisher_key)) |publisher| {
                    //     publisher.publish(message) catch |err| {
                    //         log.err("could not publish message {any}", .{err});
                    //         message.deref();
                    //     };
                    //     return;
                    // }

                    // const publisher = try self.allocator.create(Publisher);
                    // errdefer self.allocator.destroy(publisher);

                    // publisher.* = try Publisher.init(
                    //     self.allocator,
                    //     publisher_key,
                    //     conn.connection_id,
                    //     constants.publisher_max_queue_capacity,
                    //     message.topicName(),
                    // );
                    // errdefer publisher.deinit();

                    // try self.publishers.put(publisher_key, publisher);

                    // // check if the bus even exists
                    // const bus_manager = self.node.bus_manager;
                    // const bus = try bus_manager.findOrCreate(message.topicName());
                    // try bus.addPublisher(publisher);

                    // publisher.publish(message) catch |err| {
                    //     log.err("could not publish message {any}", .{err});
                    //     message.deref();
                    // };
                },
                else => {
                    //                     message.deref();
                },
            }
        }

        assert(conn.inbox.count == 0);
    }

    fn distribute(self: *Self, conn: *Connection) !void {
        self.connection_messages_mutex.lock();
        defer self.connection_messages_mutex.unlock();

        if (self.connection_messages.map.get(conn.connection_id)) |messages| {
            conn.outbox.concatenateAvailable(messages);
        }
    }

    fn cleanupUninitializedConnection(self: *Self, tmp_id: uuid.Uuid, conn: *Connection) !void {
        log.debug("remove uninitialized connection called", .{});

        _ = self.uninitialized_connections.remove(tmp_id);
        log.info("worker: {} removed uninitialized_connection {}", .{ self.id, conn.connection_id });

        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        self.removeConnection(conn);
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
                log.err("client uninitialized_connection tick err {any}", .{err});
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
                log.err("client connection tick err {any}", .{err});
                unreachable;
            };
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
        const tmp_conn_id = uuid.v7.new();
        conn.* = try Connection.init(
            0,
            self.id,
            .outbound,
            self.io,
            socket,
            self.allocator,
            self.memory_pool,
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

    pub fn ping(self: *Self, conn: *Connection, options: PingOptions, timeout_ns: i64) !void {
        _ = options;
        // FIX: this will just crash and that is bad
        assert(conn.state == .connected);

        const ping_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(ping_message);

        ping_message.* = Message.new();
        ping_message.headers.message_type = .ping;
        ping_message.setTransactionId(uuid.v7.new());
        ping_message.ref();
        errdefer ping_message.deref();

        var channel = UnbufferedChannel(*Message).new();

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, ping_message);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(ping_message.transactionId(), &channel);
        }

        const pong_message = channel.timedReceive(@intCast(timeout_ns)) catch |err| switch (err) {
            error.Timeout => {
                self.transactions_mutex.lock();
                defer self.transactions_mutex.unlock();

                _ = self.transactions.remove(ping_message.transactionId());
                return err;
            },
            else => unreachable,
        };

        pong_message.deref();
        self.memory_pool.destroy(pong_message);
    }
};

pub const Transaction = struct {
    const Self = @This();
    id: uuid.Uuid,
    channel: *UnbufferedChannel(*Message),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, id: uuid.Uuid) !Self {
        const channel = try allocator.create(UnbufferedChannel(*Message));
        errdefer allocator.destroy(channel);

        channel.* = UnbufferedChannel(*Message).new();

        return Self{
            .allocator = allocator,
            .id = id,
            .channel = channel,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.channel);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.start();
    defer client.close();
}
