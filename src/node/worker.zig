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

const IO = @import("../io.zig").IO;
const Node = @import("./node.zig").Node;

const Connection = @import("../protocol/connection.zig").Connection;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;

// const Publisher = @import("../pubsub/publisher.zig").Publisher;
// const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;

const WorkerState = enum {
    running,
    closing,
    closed,
};

const Envelope = struct {
    connection_id: u128,
    message: *Message,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    connection_messages: std.AutoHashMap(u128, *RingBuffer(*Message)),
    connection_outbox_buffers_mutex: std.Thread.Mutex,
    connection_outbox_buffers: std.AutoHashMap(u128, *RingBuffer(*Message)),
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    done_channel: *UnbufferedChannel(bool),
    id: usize,
    io: *IO,
    node: *Node,
    outbox_channel: *BufferedChannel(Envelope),
    state: WorkerState,
    transactions: std.AutoHashMap(u128, *UnbufferedChannel(*Message)),
    uninitialized_connections: std.AutoHashMap(uuid.Uuid, *Connection),
    // publishers: std.AutoHashMap(u128, *Publisher),
    // subscribers: std.AutoHashMap(u128, *Subscriber),
    inbox: *RingBuffer(*Message),
    inbox_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, id: usize, node: *Node) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const outbox_channel = try allocator.create(BufferedChannel(Envelope));
        errdefer allocator.destroy(outbox_channel);

        outbox_channel.* = try BufferedChannel(Envelope).init(allocator, 5_000);
        errdefer outbox_channel.deinit();

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, node.memory_pool.capacity);
        errdefer inbox.deinit();

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
            .connection_outbox_buffers = std.AutoHashMap(u128, *RingBuffer(*Message)).init(allocator),
            .connection_outbox_buffers_mutex = std.Thread.Mutex{},
            .outbox_channel = outbox_channel,
            .connection_messages = std.AutoHashMap(u128, *RingBuffer(*Message)).init(allocator),
            .transactions = std.AutoHashMap(u128, *UnbufferedChannel(*Message)).init(allocator),
            // .publishers = std.AutoHashMap(u128, *Publisher).init(allocator),
            // .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
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
        var connection_outbox_buffers_iter = self.connection_outbox_buffers.valueIterator();
        while (connection_outbox_buffers_iter.next()) |entry| {
            const ring_buffer = entry.*;

            while (ring_buffer.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.node.memory_pool.destroy(message);
            }

            ring_buffer.deinit();
            self.allocator.destroy(ring_buffer);
        }

        // drain the outbox channel if it isn't empty
        if (!self.outbox_channel.isEmpty()) {
            while (self.outbox_channel.buffer.dequeue()) |envelope| {
                const message = envelope.message;
                message.deref();
                if (message.refs() == 0) self.node.memory_pool.destroy(message);
            }
        }

        // var publishers_iter = self.publishers.valueIterator();
        // while (publishers_iter.next()) |publisher_entry| {
        //     const publisher = publisher_entry.*;
        //     self.allocator.destroy(publisher);
        // }

        // var subscribers_iter = self.subscribers.valueIterator();
        // while (subscribers_iter.next()) |subscriber_entry| {
        //     const subscriber = subscriber_entry.*;
        //     self.allocator.destroy(subscriber);
        // }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.node.memory_pool.destroy(message);
        }

        self.outbox_channel.deinit();
        self.connection_outbox_buffers.deinit();
        self.connections.deinit();
        self.io.deinit();
        self.uninitialized_connections.deinit();
        self.transactions.deinit();
        // self.publishers.deinit();
        // self.subscribers.deinit();
        self.inbox.deinit();

        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.outbox_channel);
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
                    self.io.run_for_ns(100 * std.time.ns_per_us) catch unreachable;
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
                    try self.cleanupConnection(conn);
                    continue;
                }

                conn.tick() catch |err| {
                    log.err("could not tick connection error: {any}", .{err});
                    continue;
                };

                try self.process(conn);
            }
        }
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
        log.debug("remove connection called", .{});

        _ = self.connections.remove(conn.connection_id);

        log.info("worker: {} removed connection {}", .{ self.id, conn.connection_id });
        conn.deinit();
        self.allocator.destroy(conn);
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

    fn process(self: *Self, conn: *Connection) !void {
        // check to see if there are messages
        if (conn.inbox.count == 0) return;

        while (conn.inbox.dequeue()) |message| {
            // defer self.node.processed_messages_count += 1;
            // defer {
            //     // _ = self.node.metrics.messages_processed.fetchAdd(1, .seq_cst);
            //     // self.node.metrics.last_updated_at_ms = std.time.milliTimestamp();
            //     message.deref();
            //     if (message.refs() == 0) self.node.memory_pool.destroy(message);
            // }
            switch (message.headers.message_type) {
                .accept => {
                    defer {
                        // _ = self.node.metrics.messages_processed.fetchAdd(1, .seq_cst);
                        // self.node.metrics.last_updated_at_ms = std.time.milliTimestamp();
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
                },
                .ping => {
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
                    // message.ref();
                },
                .pong => {
                    defer {
                        message.deref();
                        if (message.refs() == 0) self.node.memory_pool.destroy(message);
                    }

                    log.debug("received pong from origin_id: {}, connection_id: {}", .{
                        message.headers.origin_id,
                        message.headers.connection_id,
                    });
                },
                else => {
                    self.inbox_mutex.lock();
                    defer self.inbox_mutex.unlock();

                    assert(message.refs() == 1);
                    self.inbox.enqueue(message) catch |err| {
                        // message.ref();
                        try conn.inbox.enqueue(message);

                        log.err("could not enqueue message {any}", .{err});
                        // message.deref();
                    };
                },
                // .publish => {
                // // 1. get publisher from publishers
                // // 2. publish to publisher topic
                // const publisher_key = utils.generateKey(message.topicName(), conn.connection_id);
                // var publisher: *Publisher = undefined;
                // if (self.publishers.get(publisher_key)) |p| {
                //     publisher = p;
                // } else {
                //     const topic = try self.node.findOrCreateTopic(message.topicName(), .{});

                //     publisher = try self.allocator.create(Publisher);
                //     errdefer self.allocator.destroy(publisher);

                //     publisher.* = Publisher.new(conn.connection_id, topic);
                //     try self.publishers.put(publisher_key, publisher);
                // }

                // try publisher.publish(message);

                // FIX: How can i hook up messages received in the worker to more of a global router?
                // this is a point of high contention as it requires the worker and the router to sync
                // at some point during the processing of the message.
                //
                // 1. have the worker fill up a queue and simply wait for the queue to be
                // processed by the node.
                // 2. Make the node perform a collection/processing operation. This means that there would still
                // be a central thread in which all messages are processed but it would basically mean that the
                // workers are restricted to just sending/receiving messages (ticking connections)
                //     the benefit to having a central processing system is that the contention points would be to
                //     copy the messages received in the worker's inbox to the node. I think this is going to require
                //     the reintroduction of the `Envelope` idea but instead of it being used to share memory_pool
                //     information it would just include information from where the message came from. (is this true?)
                // 3. Workers are in charge and the node is just the connective tissue that can handle routing messages
                // between workers.
                //     a. worker 1 recv message
                //     b. worker 1 push to node queue
                //     c. node process message
                //     d. node route message to worker 4
                // 4. More aggressive sharing of memory between workers. What I mean by this is that we could
                // have the workers access global (to the process) resources. I think that this would lead to high
                // contention touch points. For example, publishing a message would talk to a global topic manager,
                // which would require both the topic_manager and topic to be locked as the message was processed
                // or routed.
                // },
                // .subscribe => {},
                // .unsubscribe => {},
                // else => {},
            }
        }

        assert(conn.inbox.count == 0);
    }

    pub fn distribute(self: *Self, conn: *Connection) !void {
        // self.mutex.lock();
        // defer self.mutex.unlock();

        _ = self;
        _ = conn;

        // self.mutex.lock();
        // defer self.mutex.unlock();
        // if (worker.connection_messages.get(conn.connection_id)) |messages| {
        //     conn.outbox.concatenateAvailable(messages);
        // }
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
