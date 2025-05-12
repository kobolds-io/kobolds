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
    done_channel: *UnbufferedChannel(bool),
    state: WorkerState,
    id: usize,
    node: *Node,
    io: *IO,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),

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
            .uninitialized_connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
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

        var uninitialized_connections_iter = self.uninitialized_connections.valueIterator();
        while (uninitialized_connections_iter.next()) |entry| {
            const connection = entry.*;

            connection.deinit();
            self.allocator.destroy(connection);
        }

        self.connections.deinit();
        self.uninitialized_connections.deinit();
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
                while (!self.closeAllConnections()) {}
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    pub fn tick(self: *Self) !void {
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
                    try self.cleanupConnection(conn);
                    continue;
                }

                conn.tick() catch |err| {
                    log.err("could not tick connection error: {any}", .{err});
                    continue;
                };

                try self.gather(conn);
            }
        }

        // try self.process();
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
        accept_headers.accepted_connection_id = conn_id;
        accept_headers.connection_id = self.node.id;

        connection.state = .connected;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.id, conn_id });
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
    }

    fn removeConnection(self: *Self, conn: *Connection) void {
        log.debug("remove connection called", .{});
        // self.connections_mutex.lock();
        // defer self.connections_mutex.unlock();

        _ = self.connections.remove(conn.connection_id);

        log.info("worker: {} removed connection {}", .{ self.id, conn.connection_id });
        conn.deinit();
        self.allocator.destroy(conn);
    }

    fn cleanupUninitializedConnection(self: *Self, tmp_id: uuid.Uuid, conn: *Connection) !void {
        log.debug("remove uninitialized connection called", .{});
        // self.connections_mutex.lock();
        // defer self.connections_mutex.unlock();

        _ = self.uninitialized_connections.remove(tmp_id);
        log.info("worker: {} removed uninitialized_connection {}", .{ self.id, conn.connection_id });

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

        //     if (publisher.conn_id == conn.connection_id) {
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

        //     if (subscriber.conn_id == conn.connection_id) {
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
            defer {
                message.deref();
                if (message.refs() == 0) self.node.memory_pool.destroy(message);
            }
            switch (message.headers.message_type) {
                .ping => {
                    // Since this is a `ping` we don't need to do any extra work to figure out how to respond
                    message.headers.message_type = .pong;
                    message.headers.connection_id = self.node.id;
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
                .subscribe => {
                    // defer message.deref();
                    // log.debug("subscribe message received!", .{});

                    // const subscriber_key = utils.generateKey(message.topicName(), conn.connection_id);

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
                    //     conn.connection_id,
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

                    // const subscriber_key = utils.generateKey(message.topicName(), conn.connection_id);

                    // if (self.subscribers.get(subscriber_key)) |subscriber| {
                    //     try subscriber.unsubscribe();
                    //     _ = self.subscribers.remove(subscriber_key);
                    // }

                    // TODO: send unsubscribe reply
                },
                else => {
                    //                     message.deref();
                },
            }
        }

        assert(conn.inbox.count == 0);
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
