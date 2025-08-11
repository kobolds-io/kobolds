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
const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Connection = @import("../protocol/connection.zig").Connection;

const Topic = @import("../pubsub/topic.zig").Topic;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
const SubscriberCallback = *const fn (message: *Message) void;

const Service = @import("../services/service.zig").Service;
const Advertiser = @import("../services/advertiser.zig").Advertiser;
const Requestor = @import("../services/requestor.zig").Requestor;
const AdvertiserCallback = *const fn (request: *Message, reply: *Message) void;

const PingOptions = struct {};
const PublishOptions = struct {};
const SubscribeOptions = struct {};
const RequestOptions = struct {};
const AdvertiseOptions = struct {};

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
    topics_mutex: std.Thread.Mutex,
    subscriber_callbacks: std.AutoHashMap(u128, SubscriberCallback),
    services: std.StringHashMap(*Service),
    services_mutex: std.Thread.Mutex,
    advertiser_callbacks: std.AutoHashMap(u128, AdvertiserCallback),
    inbox: *RingBuffer(*Message),
    inbox_mutex: std.Thread.Mutex,

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
        errdefer memory_pool.deinit();

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, 10_000);
        errdefer inbox.deinit();

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
            .topics_mutex = std.Thread.Mutex{},
            .subscriber_callbacks = std.AutoHashMap(u128, SubscriberCallback).init(allocator),
            .services = std.StringHashMap(*Service).init(allocator),
            .services_mutex = std.Thread.Mutex{},
            .advertiser_callbacks = std.AutoHashMap(u128, AdvertiserCallback).init(allocator),
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
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

        // clients can only connect to nodes
        assert(config.peer_type == .node);

        const conn = try self.addOutboundConnection(config);
        errdefer self.disconnect(conn);

        const deadline = std.time.nanoTimestamp() + timeout_ns;
        while (deadline > std.time.nanoTimestamp()) {
            if (conn.state == .connected and conn.connection_id != 0) return conn;

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
        try self.tickConnections();
        try self.tickUninitializedConnections();
        try self.processInboundConnectionMessages();
        try self.processUninitializedConnectionMessages();
        try self.processClientMessages();
        try self.aggregateOutboundMessages();
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

            if (conn.state == .connected and conn.connection_id != 0) {
                // the connection is now valid and ready for events
                // move the connection to the regular connections map
                try self.connections.put(conn.connection_id, conn);
                // remove the connection from the uninitialized_connections map
                assert(self.uninitialized_connections.remove(tmp_id));
            }
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
                    .pong => try self.handlePongMessage(conn, message),
                    else => {
                        self.inbox_mutex.lock();
                        defer self.inbox_mutex.unlock();

                        self.inbox.enqueue(message) catch |err| {
                            log.err("could not enqueue message {any}", .{err});
                            try conn.inbox.prepend(message);
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
        while (uninitialized_connections_iter.next()) |uninitialized_connection_entry| {
            const conn = uninitialized_connection_entry.*;

            if (conn.inbox.count == 0) continue;
            while (conn.inbox.dequeue()) |message| {
                // if this message has more than a single ref, something has not been initialized
                // or deinitialized correctly.
                assert(message.refs() == 1);

                switch (message.headers.message_type) {
                    .accept => try self.handleAcceptMessage(conn, message),
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

    fn handleAcceptMessage(self: *Self, conn: *Connection, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        // ensure that this connection is not fully connected
        assert(conn.state != .connected);
        assert(conn.connection_id == 0);

        // An error here would be a protocol error
        assert(conn.peer_id != message.headers.origin_id);
        assert(conn.connection_id != message.headers.connection_id);

        conn.connection_id = message.headers.connection_id;
        conn.peer_id = message.headers.origin_id;

        conn.state = .connected;
        log.info("outbound_connection - origin_id: {}, connection_id: {}, remote_id: {}, peer_type: {any}", .{
            conn.origin_id,
            conn.connection_id,
            conn.peer_id,
            conn.config.outbound.peer_type,
        });
    }

    fn handlePongMessage(self: *Self, conn: *Connection, message: *Message) !void {
        _ = conn;
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.transactions_mutex.lock();
        defer self.transactions_mutex.unlock();

        // check if this pong message is part of transaction
        if (self.transactions.get(message.transactionId())) |signal| {
            message.ref();
            signal.send(message);
            _ = self.transactions.remove(message.transactionId());
        }
    }

    fn handleRequestMessage(self: *Self, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.services_mutex.lock();
        defer self.services_mutex.unlock();

        if (self.services.get(message.topicName())) |service| {
            message.ref();
            service.requests_queue.enqueue(message) catch message.deref();
        } else unreachable;
    }

    fn handleReplyMessage(self: *Self, message: *Message) !void {
        self.transactions_mutex.lock();
        defer self.transactions_mutex.unlock();

        // check if this pong message is part of transaction
        if (self.transactions.get(message.transactionId())) |signal| {
            message.ref();
            signal.send(message);
            _ = self.transactions.remove(message.transactionId());
        }
    }

    fn handlePublishMessage(self: *Self, message: *Message) !void {
        defer {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        if (self.topics.get(message.topicName())) |topic| {
            message.ref();
            topic.queue.enqueue(message) catch message.deref();
        } else unreachable;
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
        callback: SubscriberCallback,
        options: SubscribeOptions,
    ) !void {
        assert(conn.state == .connected);

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        var topic: *Topic = undefined;
        if (self.topics.get(topic_name)) |t| {
            topic = t;
        } else {
            topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);

            topic.* = try Topic.init(self.allocator, self.memory_pool, topic_name, .{});
            errdefer topic.deinit();

            try self.topics.put(topic_name, topic);
        }

        const subscriber_key = utils.generateKey(topic_name, conn.connection_id);

        try topic.addSubscriber(subscriber_key, conn.connection_id);
        errdefer _ = topic.removeSubscriber(subscriber_key);

        // Add the subscriber_callback which is tied to this THIS subscriber
        try self.subscriber_callbacks.put(subscriber_key, callback);
        errdefer _ = self.subscriber_callbacks.remove(subscriber_key);

        // try topic.subscribers.put(subscriber_key, callback);
        // errdefer _ = topic.subscribers.remove(subscriber_key);

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

    pub fn unsubscribe(
        self: *Self,
        conn: *Connection,
        signal: *Signal(*Message),
        topic_name: []const u8,
        // callback: Topic.SubscriptionCallback,
    ) !void {
        // _ = callback;
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

    pub fn request(
        self: *Self,
        conn: *Connection,
        signal: *Signal(*Message),
        topic_name: []const u8,
        body: []const u8,
        options: RequestOptions,
    ) !void {
        _ = options;
        const req = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(req);

        req.* = Message.new();
        req.headers.message_type = .request;
        req.setTransactionId(uuid.v7.new());
        req.setTopicName(topic_name);
        req.setBody(body);
        req.ref();
        errdefer req.deref();

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, req);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(req.transactionId(), signal);
        }
    }

    pub fn advertise(
        self: *Self,
        conn: *Connection,
        signal: *Signal(*Message),
        topic_name: []const u8,
        callback: AdvertiserCallback,
        options: AdvertiseOptions,
    ) !void {
        assert(conn.state == .connected);

        self.services_mutex.lock();
        defer self.services_mutex.unlock();

        var service: *Service = undefined;
        if (self.services.get(topic_name)) |s| {
            service = s;
        } else {
            service = try self.allocator.create(Service);
            errdefer self.allocator.destroy(service);

            service.* = try Service.init(self.allocator, self.memory_pool, topic_name, .{});
            errdefer service.deinit();

            try self.services.put(topic_name, service);
        }

        const advertiser_key = utils.generateKey(topic_name, conn.connection_id);

        try service.addAdvertiser(advertiser_key, conn.connection_id);
        errdefer _ = service.removeAdvertiser(advertiser_key);

        // Add the advertiser_callback which is tied to this THIS advertiser
        try self.advertiser_callbacks.put(advertiser_key, callback);
        errdefer _ = self.advertiser_callbacks.remove(advertiser_key);

        const advertise_message = try self.memory_pool.create();
        errdefer self.memory_pool.destroy(advertise_message);

        advertise_message.* = Message.new();
        advertise_message.headers.message_type = .advertise;
        advertise_message.setTransactionId(uuid.v7.new());
        advertise_message.setTopicName(topic_name);
        advertise_message.ref();
        errdefer advertise_message.deref();

        {
            self.connection_messages_mutex.lock();
            defer self.connection_messages_mutex.unlock();

            try self.connection_messages.append(conn.connection_id, advertise_message);
        }

        {
            self.transactions_mutex.lock();
            defer self.transactions_mutex.unlock();

            try self.transactions.put(advertise_message.transactionId(), signal);
        }

        // register the callback to be executed when we receive a new message on the matching topic
        _ = options;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.start();
    defer client.close();
}
