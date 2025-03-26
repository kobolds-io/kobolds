const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Worker);
const posix = std.posix;

// Deps
const uuid = @import("uuid");

const constants = @import("../constants.zig");
const Broker = @import("./broker.zig").Broker;
const IO = @import("../io.zig").IO;
const TopicManager = @import("./topic_manager.zig").TopicManager;
const Subscriber = @import("./subscriber.zig").Subscriber;

// Protocol
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;
const Connection = @import("../protocol/connection.zig").Connection;

// Datastructure
const Channel = @import("../data_structures/channel.zig").Channel;
const UnmanagedQueue = @import("../data_structures/unmanaged_queue.zig").UnmanagedQueue;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const WorkerState = enum {
    running,
    close,
    closed,
};

const WorkerConfig = struct {
    node_id: uuid.Uuid,
    id: u32,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    broker: *Broker,
    config: WorkerConfig,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    message_pool: *MessagePool,
    io: *IO,
    state: WorkerState,
    pending_messages: MessageQueue,
    topic_manager: *TopicManager,
    subscribers: std.AutoHashMap(uuid.Uuid, *Subscriber),

    pub fn init(
        allocator: std.mem.Allocator,
        broker: *Broker,
        message_pool: *MessagePool,
        topic_manager: *TopicManager,
        config: WorkerConfig,
    ) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        return Self{
            .allocator = allocator,
            .broker = broker,
            .config = config,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .message_pool = message_pool,
            .pending_messages = MessageQueue.new(),
            .io = io,
            .state = .closed,
            .topic_manager = topic_manager,
            .subscribers = std.AutoHashMap(uuid.Uuid, *Subscriber).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // TODO: loop over and close all connections
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |connection_ptr| {
            const connection = connection_ptr.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        self.pending_messages.reset();

        // deinit
        self.connections.deinit();
        self.io.deinit();

        // dealloc
        self.allocator.destroy(self.io);
        // self.allocator.destroy(self.aggregated_topic_messages);
    }

    pub fn run(self: *Self, ready_chan: *Channel(anyerror!bool)) void {
        assert(self.state != .running);

        log.info("worker {} running", .{self.config.id});

        self.state = .running;
        ready_chan.send(true);

        while (true) {
            switch (self.state) {
                .running => {
                    // loop over all connections and gather their messages
                    var connections_iter = self.connections.iterator();
                    while (connections_iter.next()) |entry| {
                        const conn = entry.value_ptr.*;

                        self.gatherPendingMessages(conn) catch unreachable;
                        // try self.distribute(conn);

                        conn.tick() catch unreachable;

                        if (conn.state == .closed) {
                            _ = self.removeConnection(conn);
                            continue;
                        }
                    }

                    self.processPendingMessages();
                    // self.escalatePendingMessages();

                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
                },
                .close => {
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

                    if (all_connections_closed) {
                        self.state = .closed;
                        log.debug("closed all connections", .{});
                    }
                },
                .closed => return,
            }
        }
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

    fn gatherPendingMessages(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        self.pending_messages.concatenate(&conn.inbox);
        conn.inbox.clear();

        assert(conn.inbox.count == 0);
    }

    fn processPendingMessages(self: *Self) void {
        // Topic messages is used to aggregate messages for each topic in this batch of pending messages.
        // Since each message is destined for a single topic, we can use a message queue to link them together like
        // a chain and not have to do any special reallocations.
        var topic_messages = std.StringHashMap(*MessageQueue).init(self.allocator);
        defer topic_messages.deinit();

        while (self.pending_messages.dequeue()) |message| {
            defer {
                if (message.refs() == 0) self.message_pool.destroy(message);
            }

            const msg_type = message.headers.message_type;

            switch (msg_type) {
                .ping => {
                    defer self.broker.processed_messages_count += 1;

                    // Modify message in-place to avoid extra allocation
                    message.headers.message_type = .pong;
                    message.setTransactionId(message.transactionId());
                    message.setErrorCode(.ok);

                    if (self.connections.get(message.headers.origin_id)) |conn| {
                        message.ref();
                        if (conn.outbox.enqueue(message)) |_| {
                            // Success, continue
                        } else |err| {
                            log.err("Failed to enqueue message to outbox: {}", .{err});
                            message.deref(); // Undo reference if enqueue fails
                        }
                    } else {
                        log.err("Unable to handle ping message, connection not found", .{});
                    }
                },
                .publish => {
                    defer self.broker.processed_messages_count += 1;
                    defer message.deref();

                    // const topic_name = message.topicName();
                    //
                    // if (topic_messages.get(topic_name)) |entry| {
                    //     entry.enqueue(message);
                    // } else {
                    //     var q = MessageQueue.new();
                    //     q.enqueue(message);
                    //     topic_messages.put(topic_name, &q) catch unreachable;
                    // }
                },
                // .subscribe, .unsubscribe => {
                // },
                else => {},
            }
        }

        // var topic_messages_iter = topic_messages.iterator();
        // while (topic_messages_iter.next()) |entry| {
        //     const topic_name = entry.key_ptr.*;
        //     const queue = entry.value_ptr.*;
        //     // log.debug("topic_name: {s}, # of messages {}", .{ topic_name, queue.count });
        //     self.topic_manager.enqueueTopicMessages(topic_name, queue) catch unreachable;
        //
        //     // while (queue.dequeue()) |m| {
        //     //     m.deref();
        //     //     if (m.refs() == 0) self.message_pool.destroy(m);
        //     // }
        // }
    }

    fn escalatePendingMessages(self: *Self) void {
        if (self.pending_messages.count == 0) return;

        // lock the pending messages queue on the broker so no other worker clobbers it.
        // This entire function should be close to instantaneous because we are just moving pointers around

        // self.broker.pending_messages_mutex.lock();
        // defer self.broker.pending_messages_mutex.unlock();

        // put the gathered messages on the broker
        self.broker.pending_messages.concatenate(&self.pending_messages);
        self.pending_messages.reset();
    }

    pub fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const conn_id = uuid.v7.new();

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
        const accept_message = self.message_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .close;
            return;
        };

        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.config.node_id;

        connection.state = .connected;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.config.id, conn_id });
    }

    pub fn removeConnection(self: *Self, conn: *Connection) bool {
        defer log.info("worker: {} removed connection {}", .{ self.config.id, conn.origin_id });
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        conn.deinit();
        return self.connections.remove(conn.origin_id);

        // const removed_from_connection_messages = self.connection_messages.remove(conn.origin_id);
        // const removed_from_connections = self.connections.remove(conn.origin_id);
        //
        // return removed_from_connection_messages and removed_from_connections;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 100);
    defer message_pool.deinit();

    var topic_manager = TopicManager.init(allocator, &message_pool);
    defer topic_manager.deinit();

    var broker = try Broker.init(allocator, &message_pool, &topic_manager, .{});
    defer broker.deinit();

    const config = WorkerConfig{
        .id = 0,
        .node_id = 1,
    };

    var worker = try Worker.init(allocator, &broker, &message_pool, &topic_manager, config);
    defer worker.deinit();
}
