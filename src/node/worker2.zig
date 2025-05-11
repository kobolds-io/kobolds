const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const posix = std.posix;
const log = std.log.scoped(.Worker);

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const IO = @import("../io.zig").IO;
const Node = @import("./node2.zig").Node;

const Connection = @import("../protocol/connection2.zig").Connection;
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
    done_channel: *UnbufferedChannel(bool),
    state: WorkerState,
    id: usize,
    node: *Node,
    io: *IO,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    pub fn init(allocator: std.mem.Allocator, id: usize, node: *Node) !Self {
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

        return Self{
            .id = id,
            .allocator = allocator,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
            .node = node,
            .io = io,
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .connections_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        self.connections.deinit();
        self.io.deinit();

        self.allocator.destroy(self.io);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
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
                self.connections_mutex.lock();
                defer self.connections_mutex.unlock();

                var connections_iterator = self.connections.valueIterator();
                while (connections_iterator.next()) |entry| {
                    const connection = entry.*;

                    connection.deinit();
                    self.allocator.destroy(connection);
                }
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    pub fn tick(self: *Self) !void {

        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;

            try self.gather(conn);

            conn.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };

            if (conn.state == .closed) {
                try self.cleanupConnection(conn);
                continue;
            }
        }

        // try self.process();
    }

    pub fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const conn_id = uuid.v7.new();
        connection.* = try Connection.init(
            conn_id,
            .node,
            self.io,
            socket,
            self.allocator,
            self.node.memory_pool,
        );
        errdefer connection.deinit();

        connection.state = .connecting;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, connection);
        errdefer _ = self.connections.remove(conn_id);

        const accept_message = self.node.memory_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .close;
            return;
        };
        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.node.id;

        connection.state = .connected;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.id, conn_id });
    }

    fn removeConnection(self: *Self, conn: *Connection) void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        _ = self.connections.remove(conn.origin_id);

        log.info("worker: {} removed connection {}", .{ self.id, conn.origin_id });
        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        // Clean up publishers and subscribers associated with this connection
        // var conn_publisher_keys = std.ArrayList(u128).init(self.allocator);
        // defer conn_publisher_keys.deinit();

        // var publishers_iter = self.publishers.iterator();
        // while (publishers_iter.next()) |publisher_entry| {
        //     const publisher_key = publisher_entry.key_ptr.*;
        //     const publisher = publisher_entry.value_ptr.*;

        //     // FIX: the publisher may be in the middle of publishing
        //     // we should ensure that it is safe to destroy this publisher

        //     if (publisher.conn_id == conn.origin_id) {
        //         if (self.node.bus_manager.get(publisher.topic_name)) |bus| {
        //             _ = try bus.removePublisher(publisher.key);
        //         }

        //         publisher.deinit();
        //         self.allocator.destroy(publisher);
        //         try conn_publisher_keys.append(publisher_key);
        //     }
        // }

        // for (conn_publisher_keys.items) |publisher_key| {
        //     _ = self.publishers.remove(publisher_key);
        // }

        // var conn_subscriber_keys = std.ArrayList(u128).init(self.allocator);
        // defer conn_subscriber_keys.deinit();

        // var subscribers_iter = self.subscribers.iterator();
        // while (subscribers_iter.next()) |subscriber_entry| {
        //     const subscriber_key = subscriber_entry.key_ptr.*;
        //     const subscriber = subscriber_entry.value_ptr.*;

        //     if (subscriber.conn_id == conn.origin_id) {
        //         subscriber.unsubscribe() catch @panic("subscriber could not unsubscribe from bus");
        //         subscriber.deinit();
        //         self.allocator.destroy(subscriber);
        //         try conn_subscriber_keys.append(subscriber_key);
        //     }
        // }

        // for (conn_subscriber_keys.items) |subscriber_key| {
        //     _ = self.subscribers.remove(subscriber_key);
        // }

        self.removeConnection(conn);
    }

    fn gather(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        while (conn.inbox.dequeue()) |message| {
            // defer self.node.processed_messages_count += 1;

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
                    // // get the publisher's key
                    // const publisher_key = utils.generateKey(message.topicName(), conn.origin_id);
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
                    //     conn.origin_id,
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
                .subscribe => {
                    // defer message.deref();
                    // log.debug("subscribe message received!", .{});

                    // const subscriber_key = utils.generateKey(message.topicName(), conn.origin_id);

                    // // check if this connection is already subscribed to this topic
                    // if (self.subscribers.contains(subscriber_key)) {
                    //     // this is not allowed
                    //     const reply = try Message.create(self.message_pool);
                    //     errdefer reply.deref();

                    //     reply.headers.message_type = .reply;
                    //     reply.setTopicName(message.topicName());
                    //     reply.setTransactionId(message.transactionId());
                    //     reply.setErrorCode(.bad_request); // TODO: this should be a better error

                    //     try conn.outbox.enqueue(reply);
                    //     return;
                    // }

                    // const bus_manager = self.node.bus_manager;
                    // const bus = try bus_manager.findOrCreate(message.topicName());

                    // const subscriber = try self.allocator.create(Subscriber);
                    // errdefer self.allocator.destroy(subscriber);

                    // subscriber.* = try Subscriber.init(
                    //     self.allocator,
                    //     subscriber_key,
                    //     conn.origin_id,
                    //     constants.subscriber_max_queue_capacity,
                    //     bus,
                    // );
                    // errdefer subscriber.deinit();

                    // try self.subscribers.put(subscriber_key, subscriber);
                    // try subscriber.subscribe();
                    // errdefer _ = subscriber.unsubscribe() catch @panic("subscriber could not unsubscribe");

                    // // Reply to the subscribe request
                    // const reply = try Message.create(self.message_pool);
                    // errdefer reply.deref();

                    // reply.headers.message_type = .reply;
                    // reply.setTopicName(message.topicName());
                    // reply.setTransactionId(message.transactionId());
                    // reply.setErrorCode(.ok);

                    // try conn.outbox.enqueue(reply);
                },
                .unsubscribe => {
                    // defer message.deref();
                    // log.debug("unsubscribe message received!", .{});

                    // const subscriber_key = utils.generateKey(message.topicName(), conn.origin_id);

                    // if (self.subscribers.get(subscriber_key)) |subscriber| {
                    //     try subscriber.unsubscribe();
                    //     _ = self.subscribers.remove(subscriber_key);
                    // }

                    // TODO: send unsubscribe reply
                },
                else => {
                    message.deref();
                    if (message.refs() == 0) self.node.memory_pool.destroy(message);
                },
            }
        }

        assert(conn.inbox.count == 0);
    }
};
