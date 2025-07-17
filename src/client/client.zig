const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const IO = @import("../io.zig").IO;

const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const Signal = @import("stdx").Signal;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;

const PingOptions = struct {};
const PublishOptions = struct {};
const SubscribeOptions = struct {};

pub const ClientConfig = struct {
    max_connections: u16 = 100,
    memory_pool_capacity: usize = 1_000,
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
    transactions: std.AutoHashMap(uuid.Uuid, *Signal(*Message)),
    transactions_mutex: std.Thread.Mutex,
    connection_messages: ConnectionMessages,
    connection_messages_mutex: std.Thread.Mutex,
    topics: std.StringHashMap(*Topic),

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

        memory_pool.* = try MemoryPool(Message).init(allocator, 100_000);
        // memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
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
            .transactions = std.AutoHashMap(uuid.Uuid, *Signal(*Message)).init(allocator),
            .transactions_mutex = std.Thread.Mutex{},
            .connection_messages = ConnectionMessages.init(allocator, memory_pool),
            .connection_messages_mutex = std.Thread.Mutex{},
            .topics = std.StringHashMap(*Topic).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.connection_messages.deinit();

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

        var topics_iterator = self.topics.valueIterator();
        while (topics_iterator.next()) |entry| {
            const topic = entry.*;

            topic.deinit();
            self.allocator.destroy(topic);
        }

        self.io.deinit();
        self.memory_pool.deinit();
        self.connections.deinit();
        self.uninitialized_connections.deinit();
        self.transactions.deinit();
        self.topics.deinit();

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

            // FIX: this is some baaaaad code. There should instead be a signal or channel that this thread could
            // wait on instead. Since this call happens on a foreground thread, an unbuffered channel seems the most
            // appropriate.
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

                try self.process(conn);

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

                try self.process(conn);
                try self.distribute(conn);
            }
        }
    }

    fn process(self: *Self, conn: *Connection) !void {
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
                .pong => {
                    message.ref();
                    self.transactions_mutex.lock();
                    defer self.transactions_mutex.unlock();

                    // check if this pong message is part of transaction
                    if (self.transactions.get(message.transactionId())) |signal| {
                        message.ref();
                        // log.info("message.refs() {}", .{message.refs()});
                        signal.send(message);
                        _ = self.transactions.remove(message.transactionId());
                    }
                },
                .publish => {
                    if (self.topics.get(message.topicName())) |topic| {
                        topic.process(message);
                    } else {
                        return error.TopicDoesNotExist;
                    }
                },
                .reply => {
                    log.debug("reply received", .{});
                    message.ref();
                    self.transactions_mutex.lock();
                    defer self.transactions_mutex.unlock();

                    // check if this pong message is part of transaction
                    if (self.transactions.get(message.transactionId())) |signal| {
                        message.ref();
                        // log.info("message.refs() {}", .{message.refs()});
                        signal.send(message);
                        _ = self.transactions.remove(message.transactionId());
                    }
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

    pub fn ping(self: *Self, conn: *Connection, signal: *Signal(*Message), options: PingOptions) !void {
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

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, ping_message);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(ping_message.transactionId(), signal);
        }
    }

    pub fn publish(
        self: *Self,
        conn: *Connection,
        topic_name: []const u8,
        body: []const u8,
        options: PublishOptions,
    ) !void {
        _ = options;
        assert(conn.state == .connected);

        const message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(message);

        message.* = Message.new();
        message.headers.message_type = .publish;
        message.setTopicName(topic_name);
        message.setBody(body);
        message.ref();

        self.connection_messages_mutex.lock();
        defer self.connection_messages_mutex.unlock();

        try self.connection_messages.append(conn.connection_id, message);
    }

    pub fn subscribe(
        self: *Self,
        conn: *Connection,
        signal: *Signal(*Message),
        topic_name: []const u8,
        callback: Topic.SubscriptionCallback,
        options: SubscribeOptions,
    ) !void {
        assert(conn.state == .connected);

        var topic: *Topic = undefined;
        if (self.topics.get(topic_name)) |t| {
            topic = t;
        } else {
            topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);

            topic.* = Topic.init(self.allocator, topic_name);
            errdefer topic.deinit();

            try self.topics.put(topic_name, topic);
        }

        // Add the callback to the list of subscribers to this topic
        const subscriber_key = utils.generateKey(topic_name, conn.connection_id);
        try topic.subscribers.put(subscriber_key, callback);
        errdefer _ = topic.subscribers.remove(subscriber_key);

        const subscribe_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(subscribe_message);

        subscribe_message.* = Message.new();
        subscribe_message.headers.message_type = .subscribe;
        subscribe_message.setTransactionId(uuid.v7.new());
        subscribe_message.setTopicName(topic_name);
        subscribe_message.ref();
        errdefer subscribe_message.deref();

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, subscribe_message);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(subscribe_message.transactionId(), signal);
        }

        // register the callback to be executed when we receive a new message on the matching topic
        _ = options;
    }

    // const subscriber = try client.subscribe(conn, topic_name, )
    // defer client.unsubscribe(subscriber);
    //
    // const subscriber = Subscriber.new();
    // defer subscriber.unsubscribe();
    //
    // subscriber.subscribe(conn, topic_name);

    pub fn unsubscribe(
        self: *Self,
        conn: *Connection,
        signal: *Signal(*Message),
        topic_name: []const u8,
        callback: Topic.SubscriptionCallback,
    ) !void {
        _ = callback;
        if (self.topics.get(topic_name)) |topic| {
            const subscriber_key = utils.generateKey(topic_name, conn.connection_id);
            _ = topic.subscribers.remove(subscriber_key);
        } else {
            return error.NotSubscribed;
        }

        const unsubscribe_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(unsubscribe_message);

        unsubscribe_message.* = Message.new();
        unsubscribe_message.headers.message_type = .unsubscribe;
        unsubscribe_message.setTransactionId(uuid.v7.new());
        unsubscribe_message.setTopicName(topic_name);
        unsubscribe_message.ref();
        errdefer unsubscribe_message.deref();

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, unsubscribe_message);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(unsubscribe_message.transactionId(), signal);
        }
    }
};

const Topic = struct {
    const Self = @This();
    const SubscriptionCallback = *const fn (message: *Message) void;
    allocator: std.mem.Allocator,
    subscribers: std.AutoHashMap(u128, SubscriptionCallback),
    topic_name: []const u8,

    pub fn init(allocator: std.mem.Allocator, topic_name: []const u8) Self {
        return Self{
            .allocator = allocator,
            .topic_name = topic_name,
            .subscribers = std.AutoHashMap(u128, SubscriptionCallback).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // var subscribers_iter = self.subscribers.valueIterator();
        // while (subscribers_iter.next()) |entry| {
        //     const subscriber = entry.*;

        //     self.allocator.destroy(subscriber);
        // }

        self.subscribers.deinit();
    }

    pub fn process(self: *Self, message: *Message) void {
        assert(message.headers.message_type == .publish);
        assert(std.mem.eql(u8, message.topicName(), self.topic_name));

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const callback = entry.*;

            callback(message);
        }
    }
};

const Publisher = struct {};
const Subscriber = struct {
    const Self = @This();

    id: u128,
    callback: *const fn (message: *Message) void,
    messages_recv: u128,
    topic: *Topic,

    pub fn new(id: uuid.Uuid, topic: *Topic, callback: *const fn (message: *Message) void) Self {
        return Self{
            .id = id,
            .callback = callback,
            .messages_recv = 0,
            .topic = topic,
        };
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.start();
    defer client.close();
}
