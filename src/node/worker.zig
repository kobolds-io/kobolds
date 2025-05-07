const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Worker);
const posix = std.posix;

// Deps
const uuid = @import("uuid");

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

const RingBuffer = @import("stdx").RingBuffer;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const Node = @import("./node.zig").Node;
const IO = @import("../io.zig").IO;

// Bus
const Publisher = @import("../bus/publisher.zig").Publisher;
const Subscriber = @import("../bus/subscriber.zig").Subscriber;

// // Pubsub
// const Publisher = @import("../pubsub/publisher.zig").Publisher;
// const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;
// const Topic = @import("../pubsub/topic.zig").Topic;
// const TopicManager = @import("../pubsub/topic_manager.zig").TopicManager;

// Protocol
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Connection = @import("../protocol/connection.zig").Connection;

// Datastructure
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const WorkerState = enum {
    running,
    close,
    closed,
};

const WorkerConfig = struct {
    node_id: uuid.Uuid,
    id: u32,
    message_pool_capacity: u32 = constants.default_worker_message_pool_capacity,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: WorkerConfig,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    io: *IO,
    message_pool: *MessagePool,
    node: *Node,
    state: WorkerState,
    publishers: std.AutoHashMap(u128, *Publisher),
    subscribers: std.AutoHashMap(u128, *Subscriber),
    mutex: std.Thread.Mutex,

    pub fn init(
        allocator: std.mem.Allocator,
        node: *Node,
        config: WorkerConfig,
    ) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, config.message_pool_capacity);
        errdefer message_pool.deinit();

        return Self{
            .allocator = allocator,
            .config = config,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .io = io,
            .message_pool = message_pool,
            .node = node,
            .state = .closed,
            .publishers = std.AutoHashMap(u128, *Publisher).init(allocator),
            .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        // TODO: loop over and close all connections
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;

            subscriber.deinit();
            self.allocator.destroy(subscriber);
        }

        var publishers_iter = self.publishers.valueIterator();
        while (publishers_iter.next()) |entry| {
            const publisher = entry.*;

            publisher.deinit();
            self.allocator.destroy(publisher);
        }

        // deinit
        self.connections.deinit();
        self.io.deinit();
        self.message_pool.deinit();
        self.publishers.deinit();
        self.subscribers.deinit();

        // dealloc
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.message_pool);
    }

    pub fn run(self: *Self, ready_chan: *UnbufferedChannel(anyerror!bool)) void {
        assert(self.state != .running);

        log.info("worker {} running", .{self.config.id});

        self.state = .running;
        ready_chan.send(true);

        while (true) {
            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
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

    pub fn tick(self: *Self) !void {
        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;

            try self.gather(conn);
            // try self.distribute(conn);

            conn.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };

            if (conn.state == .closed) {
                try self.cleanupConnection(conn);
                continue;
            }
        }

        try self.process();
    }

    pub fn close(self: *Self) void {
        if (self.state == .closed) return;
        if (self.state == .running) self.state = .close;

        // wait for the connections to all be closed
        var i: usize = 0;
        while (self.state != .closed) : (i += 1) {
            log.debug("stopping worker attempt - {}", .{i});

            std.time.sleep(1 * std.time.ns_per_ms);
        }
    }

    fn closeAllConnections(self: *Self) bool {
        // check if every connection is closed
        var all_connections_closed = true;

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
                log.err("worker conn tick err {any}", .{err});
                unreachable;
            };
        }

        return all_connections_closed;
    }

    fn gather(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        while (conn.inbox.dequeue()) |message| {
            defer self.node.processed_messages_count += 1;

            switch (message.headers.message_type) {
                .ping => {
                    // Since this is a `ping` we don't need to do any extra work to figure out how to respond
                    message.headers.message_type = .pong;
                    message.headers.origin_id = self.node.id;
                    message.setTransactionId(message.transactionId());
                    message.setErrorCode(.ok);

                    assert(message.refs() == 1);

                    if (conn.outbox.enqueue(message)) |_| {} else |err| {
                        log.err("Failed to enqueue message to outbox: {}", .{err});
                        message.deref(); // Undo reference if enqueue fails
                    }
                },
                .publish => {
                    // log.debug("received publish messaged", .{});
                    // find the producer for this connection

                    const publisher_key = utils.generateKey(message.topicName(), conn.origin_id);
                    if (self.publishers.get(publisher_key)) |publisher| {
                        try publisher.publish(message);
                        return;
                    }

                    const publisher = try self.allocator.create(Publisher);
                    errdefer self.allocator.destroy(publisher);

                    publisher.* = try Publisher.init(
                        self.allocator,
                        publisher_key,
                        conn.origin_id,
                        constants.publisher_max_queue_capacity,
                    );
                    errdefer publisher.deinit();

                    try self.publishers.put(publisher_key, publisher);

                    // check if the bus even exists
                    const bus_manager = self.node.bus_manager;
                    const bus = try bus_manager.findOrCreate(message.topicName());

                    try bus.addPublisher(publisher);

                    publisher.publish(message) catch |err| {
                        log.err("could not publish message {any}", .{err});
                        message.deref();
                    };
                },
                .subscribe => {
                    defer message.deref();
                    log.debug("subscribe message received!", .{});

                    const subscriber_key = utils.generateKey(message.topicName(), conn.origin_id);

                    // check if this connection is already subscribed to this topic
                    if (self.subscribers.contains(subscriber_key)) {
                        // this is not allowed
                        const reply = try Message.create(self.message_pool);
                        errdefer reply.deref();

                        reply.headers.message_type = .reply;
                        reply.setTopicName(message.topicName());
                        reply.setTransactionId(message.transactionId());
                        reply.setErrorCode(.bad_request); // TODO: this should be a better error

                        try conn.outbox.enqueue(reply);
                        return;
                    }

                    const bus_manager = self.node.bus_manager;
                    const bus = try bus_manager.findOrCreate(message.topicName());

                    const subscriber = try self.allocator.create(Subscriber);
                    errdefer self.allocator.destroy(subscriber);

                    subscriber.* = try Subscriber.init(
                        self.allocator,
                        subscriber_key,
                        conn.origin_id,
                        constants.subscriber_max_queue_capacity,
                        bus,
                    );
                    errdefer subscriber.deinit();

                    try self.subscribers.put(subscriber_key, subscriber);
                    try subscriber.subscribe();
                    errdefer _ = subscriber.unsubscribe() catch @panic("subscriber could not unsubscribe");

                    // Reply to the subscribe request
                    const reply = try Message.create(self.message_pool);
                    errdefer reply.deref();

                    reply.headers.message_type = .reply;
                    reply.setTopicName(message.topicName());
                    reply.setTransactionId(message.transactionId());
                    reply.setErrorCode(.ok);

                    try conn.outbox.enqueue(reply);
                },
                .unsubscribe => {
                    defer message.deref();
                    log.debug("unsubscribe message received!", .{});

                    const subscriber_key = utils.generateKey(message.topicName(), conn.origin_id);

                    if (self.subscribers.get(subscriber_key)) |subscriber| {
                        try subscriber.unsubscribe();
                        _ = self.subscribers.remove(subscriber_key);
                    }

                    // TODO: send unsubscribe reply
                },
                else => message.deref(),
            }
        }

        assert(conn.inbox.count == 0);
    }

    fn process(self: *Self) !void {
        // var publishers_iter = self.publishers.valueIterator();
        // while (publishers_iter.next()) |entry| {
        //     const publisher = entry.*;
        //     if (publisher.queue.count == 0) continue;
        //
        //     while (publisher.queue.dequeue()) |message| {
        //         try publisher.publish(message);
        //     }
        // }
        //
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;
            if (subscriber.queue.count == 0) continue;

            if (self.connections.get(subscriber.conn_id)) |conn| {
                conn.outbox.concatenateAvailable(subscriber.queue);
            } else {
                // this subscriber is bad???
                @panic("subscriber isn't tied to connection");
            }
        }
    }

    pub fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const conn_id = utils.generateUniqueId(self.config.node_id);

        connection.* = try Connection.init(conn_id, .node, self.io, socket, self.allocator, self.message_pool);
        errdefer connection.deinit();

        connection.state = .connecting;

        {
            self.connections_mutex.lock();
            defer self.connections_mutex.unlock();

            try self.connections.put(conn_id, connection);
        }
        errdefer _ = self.connections.remove(conn_id);

        // construct and accept message
        // add an accept message to this connection's inbox immediately
        const accept_message = Message.create(self.message_pool) catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .close;
            return;
        };

        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.config.node_id;

        connection.state = .connected;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.config.id, conn_id });
    }

    fn removeConnection(self: *Self, conn: *Connection) bool {
        defer log.info("worker: {} removed connection {}", .{ self.config.id, conn.origin_id });
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        conn.deinit();
        return self.connections.remove(conn.origin_id);
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        // Clean up publishers and subscribers associated with this connection
        var conn_publisher_keys = std.ArrayList(u128).init(self.allocator);
        defer conn_publisher_keys.deinit();

        var publishers_iter = self.publishers.iterator();
        while (publishers_iter.next()) |publisher_entry| {
            const publisher_key = publisher_entry.key_ptr.*;
            const publisher = publisher_entry.value_ptr.*;

            if (publisher.conn_id == conn.origin_id) {
                publisher.deinit();
                self.allocator.destroy(publisher);
                try conn_publisher_keys.append(publisher_key);
            }
        }

        for (conn_publisher_keys.items) |publisher_key| {
            _ = self.publishers.remove(publisher_key);
        }

        var conn_subscriber_keys = std.ArrayList(u128).init(self.allocator);
        defer conn_subscriber_keys.deinit();

        var subscribers_iter = self.subscribers.iterator();
        while (subscribers_iter.next()) |subscriber_entry| {
            const subscriber_key = subscriber_entry.key_ptr.*;
            const subscriber = subscriber_entry.value_ptr.*;

            if (subscriber.conn_id == conn.origin_id) {
                subscriber.unsubscribe() catch @panic("subscriber could not unsubscribe from bus");
                subscriber.deinit();
                self.allocator.destroy(subscriber);
                try conn_subscriber_keys.append(subscriber_key);
            }
        }

        for (conn_subscriber_keys.items) |subscriber_key| {
            _ = self.subscribers.remove(subscriber_key);
        }

        _ = self.removeConnection(conn);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 100);
    defer message_pool.deinit();

    var node = try Node.init(allocator, .{});
    defer node.deinit();

    const config = WorkerConfig{
        .id = 0,
        .node_id = 1,
    };

    var worker = try Worker.init(allocator, node, config);
    defer worker.deinit();
}
