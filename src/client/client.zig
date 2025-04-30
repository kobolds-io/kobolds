const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);

const uuid = @import("uuid");

const constants = @import("../constants.zig");

const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Pong = @import("../protocol/message.zig").Pong;
const Ping = @import("../protocol/message.zig").Ping;
const Reqeust = @import("../protocol/message.zig").Request;
const Reply = @import("../protocol/message.zig").Reply;
const Compression = @import("../protocol/message.zig").Compression;
const Connection = @import("../protocol/connection.zig").Connection;
const ProtocolError = @import("../errors.zig").ProtocolError;

const IO = @import("../io.zig").IO;

const Channel = @import("../data_structures/channel.zig").Channel;
const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;
const RingBuffer = @import("stdx").RingBuffer;

const TopicManager = @import("../pubsub/topic_manager.zig").TopicManager;
const Topic = @import("../pubsub/topic.zig").Topic;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;

const PingOptions = struct {};
const PublishOptions = struct {};
const SubscribeOptions = struct {};

pub const ClientConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",

    /// tcp connections are accepted on this port
    port: u16 = 8000,

    /// default compression applied to the bodies of messages
    compression: Compression = .none,

    message_pool_capacity: u32 = 1_000,

    max_connections: u16 = 10,
};

const ClientState = enum {
    running,
    close,
    closed,
};

pub const Client = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: ClientConfig,
    connection_messages: *ConnectionMessages,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    io: *IO,
    message_pool: *MessagePool,
    state: ClientState,
    uninitialized_connections_mutex: std.Thread.Mutex,
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),
    unprocessed_messages_queue: *RingBuffer(*Message),
    transactions: std.AutoHashMap(uuid.Uuid, *Channel(anyerror!*Message)),
    topic_manager: *TopicManager,
    subscribers: std.AutoHashMap(u128, *Subscriber),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const unprocessed_messages_queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(unprocessed_messages_queue);

        unprocessed_messages_queue.* = try RingBuffer(*Message).init(allocator, config.message_pool_capacity);
        errdefer unprocessed_messages_queue.deinit();

        const connection_messages = try allocator.create(ConnectionMessages);
        errdefer allocator.destroy(connection_messages);

        connection_messages.* = ConnectionMessages.init(allocator);
        errdefer connection_messages.deinit();

        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, config.message_pool_capacity);
        errdefer message_pool.deinit();

        const topic_manager = try allocator.create(TopicManager);
        errdefer allocator.destroy(topic_manager);

        topic_manager.* = TopicManager.init(allocator);
        errdefer topic_manager.deinit();

        return Self{
            .allocator = allocator,
            .config = config,
            .connection_messages = connection_messages,
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .io = io,
            .message_pool = message_pool,
            .state = .closed,
            .uninitialized_connections_mutex = std.Thread.Mutex{},
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .unprocessed_messages_queue = unprocessed_messages_queue,
            .transactions = std.AutoHashMap(uuid.Uuid, *Channel(anyerror!*Message)).init(allocator),
            .mutex = std.Thread.Mutex{},
            .topic_manager = topic_manager,
            .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |conn_ptr| {
            var conn = conn_ptr.*;
            conn.deinit();
            self.allocator.destroy(conn);
        }
        self.uninitialized_connections.deinit();

        var conn_iter = self.connections.valueIterator();
        while (conn_iter.next()) |conn_ptr| {
            var conn = conn_ptr.*;
            conn.deinit();
            self.allocator.destroy(conn);
        }
        self.connections.deinit();

        self.unprocessed_messages_queue.deinit();
        self.transactions.deinit();
        self.connection_messages.deinit();
        self.message_pool.deinit();
        self.topic_manager.deinit();
        self.subscribers.deinit();

        self.allocator.destroy(self.topic_manager);
        self.allocator.destroy(self.unprocessed_messages_queue);
        self.allocator.destroy(self.connection_messages);
        self.allocator.destroy(self.message_pool);
        self.allocator.destroy(self.io);
    }

    pub fn start(self: *Self) !void {
        var chan = Channel(bool).init();

        const th = try std.Thread.spawn(.{}, Client.runLoop, .{ self, &chan });
        th.detach();

        _ = chan.receive();
    }

    pub fn runLoop(self: *Client, ch: *Channel(bool)) void {
        assert(self.state != .running);
        self.state = .running;
        log.info("client running", .{});
        defer log.debug("client shutting down", .{});

        // var start: u64 = 0;
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // var printed: bool = false;

        ch.send(true);

        while (true) {
            switch (self.state) {
                .running => {
                    self.tick() catch |err| {
                        log.err("client tick while running error: {any}", .{err});
                        unreachable;
                    };
                    // self.io.run_for_ns(100 * std.time.ns_per_us) catch |err| {
                    //     log.err("io.run_for_ns error: {any}", .{err});
                    //     unreachable;
                    // };

                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                        log.err("io.run_for_ns error: {any}", .{err});
                        unreachable;
                    };
                },
                .close => {
                    // if not all the connections are closed, we will simply try again on the next tick
                    if (self.closeAllConnections()) {
                        self.state = .closed;
                        log.debug("closed all connections", .{});
                    }
                },
                .closed => return,
            }
        }
    }

    pub fn stop(self: *Self) void {
        if (self.state == .closed) return;
        if (self.state == .running) self.state = .close;

        // wait for the connections to all be closed
        var i: usize = 0;
        while (self.state != .closed) : (i += 1) {
            log.debug("stopping client attempt - {}", .{i});

            std.time.sleep(1 * std.time.ns_per_ms);
        }
    }

    fn closeAllConnections(self: *Self) bool {

        // check if every connection is closed
        var all_connections_closed = true;

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |conn_ptr| {
            var conn = conn_ptr.*;
            switch (conn.state) {
                .closed => continue,
                .close => {
                    all_connections_closed = false;
                },
                else => {
                    conn.state = .close;
                    all_connections_closed = false;
                },
            }

            conn.tick() catch |err| {
                log.err("client tick err {any}", .{err});
                unreachable;
            };
        }

        var conn_iter = self.connections.valueIterator();
        while (conn_iter.next()) |conn_ptr| {
            var conn = conn_ptr.*;
            switch (conn.state) {
                .closed => continue,
                .close => {
                    all_connections_closed = false;
                },
                else => {
                    conn.state = .close;
                    all_connections_closed = false;
                },
            }

            conn.tick() catch |err| {
                log.err("client tick err {any}", .{err});
                unreachable;
            };
        }

        return all_connections_closed;
    }

    fn tick(self: *Self) !void {
        var uninitialized_connections_iter = self.uninitialized_connections.iterator();
        while (uninitialized_connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;
            const tmp_id = entry.key_ptr.*;

            try conn.tick();

            if (conn.state == .connected and conn.origin_id != 0) {
                // the connection is now valid and ready for events
                // move the connection to the regular connections map
                try self.connections.put(conn.origin_id, conn);

                // remove the connection from the uninitialized_connections map
                assert(self.uninitialized_connections.remove(tmp_id));
            }
        }

        // tick all of the initialized connections
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |conn_ptr| {
            const conn = conn_ptr.*;

            // log.debug("conn id {}, conn.outbox.count {}", .{ conn.origin_id, conn.outbox.count });

            try conn.tick();

            // aggregate all messages from the connection to the client inbox
            try self.gather(conn);
            try self.distribute(conn);
        }

        while (self.unprocessed_messages_queue.dequeue()) |message| {
            try self.process(message);
        }
    }

    fn process(self: *Self, message: *Message) !void {
        defer message.deref();
        switch (message.headers.message_type) {
            .pong => {
                // lookup the transaction
                if (self.transactions.get(message.transactionId())) |chan| {
                    defer _ = self.transactions.remove(message.transactionId());

                    message.ref();
                    chan.send(message);
                } else {
                    log.err("missing transaction {}", .{message.transactionId()});
                }
            },
            .publish => {
                // log.debug("received published message {any}", .{message});

                if (self.topic_manager.get(message.topicName())) |topic| {
                    try topic.publish(message);
                }
            },
            .accept => {},
            .reply => {
                log.debug("received reply message", .{});
                message.ref();
                const transaction_id = message.transactionId();
                if (self.transactions.get(transaction_id)) |chan| {
                    chan.send(message);
                }

                defer _ = self.transactions.remove(transaction_id);
            },
            else => |t| {
                log.err("received unhandled message type {any}", .{t});

                unreachable;
            },
        }
    }

    pub fn disconnect(self: *Self, conn: *Connection) void {
        _ = self;

        if (conn.state == .closed or conn.state == .close) return;
        conn.state = .close;
    }

    pub fn connect(self: *Self) !*Connection {
        assert(self.uninitialized_connections.count() + self.connections.count() + 1 <= self.config.max_connections);
        assert(self.state == .running);

        // spawn a bunch of connections

        const address = try std.net.Address.parseIp4(self.config.host, self.config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const socket = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(socket);

        // initialize the connection
        const conn = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn);

        conn.* = try Connection.init(0, .client, self.io, socket, self.allocator, self.message_pool);
        errdefer conn.deinit();

        {
            self.uninitialized_connections_mutex.lock();
            defer self.uninitialized_connections_mutex.unlock();
            try self.uninitialized_connections.put(uuid.v7.new(), conn);
        }

        // set the state to connecting so that we know the state
        conn.state = .connecting;

        self.io.connect(*Connection, conn, Connection.onConnect, conn.connect_completion, socket, address);
        conn.connect_submitted = true;

        while (conn.origin_id == 0) {
            std.time.sleep(constants.io_tick_ms * std.time.ns_per_ms);
        }

        return conn;
    }

    pub fn ping(self: *Self, conn: *Connection, options: PingOptions) !void {
        _ = options;

        const req = try Message.create(self.message_pool);
        errdefer req.deref();

        const transaction_id = uuid.v7.new();

        req.headers.message_type = .ping;
        req.headers.origin_id = conn.origin_id;
        req.setTransactionId(transaction_id);

        // validate the message
        if (req.validate()) |err_msg| {
            log.err("invalid message {s}", .{err_msg});
            return ProtocolError.InvalidMessage;
        }

        var chan = Channel(anyerror!*Message).init();

        {
            // lock the client
            self.mutex.lock();
            defer self.mutex.unlock();

            // register transaction
            try self.transactions.put(transaction_id, &chan);

            try conn.outbox.enqueue(req);
        }

        const tx = chan.receive();
        const rep = try tx;
        _ = rep;
    }

    pub fn publish(
        self: *Self,
        conn: *Connection,
        topic_name: []const u8,
        body: []const u8,
        options: PublishOptions,
    ) !void {
        _ = options;

        const publish_message = try Message.create(self.message_pool);
        errdefer publish_message.deref();

        publish_message.headers.message_type = .publish;
        publish_message.headers.origin_id = conn.origin_id;
        publish_message.setTopicName(topic_name);
        publish_message.setBody(body);

        try conn.outbox.enqueue(publish_message);
    }

    pub fn subscribe(
        self: *Self,
        conn: *Connection,
        topic_name: []const u8,
        callback: Subscriber.SubscriptionCallback,
        options: SubscribeOptions,
    ) !*Subscriber {
        _ = options;

        const subscribe_req = try Message.create(self.message_pool);
        errdefer subscribe_req.deref();

        const transaction_id = uuid.v7.new();

        subscribe_req.headers.message_type = .subscribe;
        subscribe_req.headers.origin_id = conn.origin_id;
        subscribe_req.setTopicName(topic_name);
        subscribe_req.setTransactionId(transaction_id);

        // validate the message
        if (subscribe_req.validate()) |err_msg| {
            log.err("invalid message {s}", .{err_msg});
            return ProtocolError.InvalidMessage;
        }

        const subscribe_rep = try self.request(conn, subscribe_req);

        if (subscribe_rep.errorCode() != .ok) {
            return error.UnableToSubscribe;
        }

        const topic_manager = self.topic_manager;
        var topic: *Topic = undefined;

        if (topic_manager.get(topic_name)) |t| {
            topic = t;
        } else {
            topic = topic_manager.create(topic_name) catch |err| switch (err) {
                // BUG: Someone has created this topic during this tick. Should handle this better.
                // I think that adding the message back into the queue and just retry it. This runs
                // the risk of getting into an unhandledable loop
                error.TopicExists => topic_manager.get(topic_name).?,
                else => unreachable,
            };
        }

        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = try Subscriber.init(self.allocator, conn.origin_id, topic);
        errdefer subscriber.deinit();

        // Override the default callback of the subscriber
        subscriber.callback = callback;
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.subscribers.put(subscriber.key, subscriber);
        errdefer _ = self.subscribers.remove(subscriber.key);

        try subscriber.subscribe();
        errdefer subscriber.unsubscribe();

        return subscriber;
    }

    pub fn unsubscribe(self: *Self, subscriber: *Subscriber) !void {
        const unsubscribe_req = try Message.create(self.message_pool);
        errdefer unsubscribe_req.deref();

        const transaction_id = uuid.v7.new();

        unsubscribe_req.headers.message_type = .unsubscribe;
        unsubscribe_req.headers.origin_id = subscriber.conn_id;
        unsubscribe_req.setTopicName(subscriber.topic.topic_name);
        unsubscribe_req.setTransactionId(transaction_id);

        // validate the message
        if (unsubscribe_req.validate()) |err_msg| {
            log.err("invalid message {s}", .{err_msg});
            return ProtocolError.InvalidMessage;
        }

        const conn = self.connections.get(subscriber.conn_id).?;

        const unsubscribe_rep = try self.request(conn, unsubscribe_req);

        if (unsubscribe_rep.errorCode() != .ok) {
            return error.UnableToUnsubscribe;
        }

        subscriber.unsubscribe();

        self.mutex.lock();
        defer self.mutex.unlock();

        _ = self.subscribers.remove(subscriber.key);

        subscriber.deinit();
        self.allocator.destroy(subscriber);
    }

    fn request(self: *Self, conn: *Connection, req: *Message) !*Message {
        assert(req.transactionId() != 0);

        var chan = Channel(anyerror!*Message).init();

        {
            // lock the client
            self.mutex.lock();
            defer self.mutex.unlock();

            // register transaction
            try self.transactions.put(req.transactionId(), &chan);

            try conn.outbox.enqueue(req);
        }

        const tx = chan.receive();
        const rep = try tx;

        return rep;
    }

    pub fn gather(self: *Self, conn: *Connection) !void {
        try self.unprocessed_messages_queue.concatenate(conn.inbox);
    }

    pub fn distribute(self: *Self, conn: *Connection) !void {
        _ = self;
        _ = conn;
    }
};

test "connect" {
    const allocator = testing.allocator;
    const config = ClientConfig{ .host = "127.0.0.1", .port = 9879, .compression = .none };

    var client = try Client.init(allocator, config);
    defer client.deinit();

    // connect needs to be able to return a valid accepted connection
    const conn = try client.connect();
    defer conn.deinit();
}
