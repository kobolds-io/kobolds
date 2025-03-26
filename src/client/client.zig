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
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;

const PingOptions = struct {};
const PublishOptions = struct {};

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
    unprocessed_messages_queue: *MessageQueue,
    transactions: std.AutoHashMap(uuid.Uuid, *Channel(anyerror!*Message)),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const unprocessed_messages_queue = try allocator.create(MessageQueue);
        errdefer allocator.destroy(unprocessed_messages_queue);

        unprocessed_messages_queue.* = MessageQueue.new();

        const connection_messages = try allocator.create(ConnectionMessages);
        errdefer allocator.destroy(connection_messages);

        connection_messages.* = ConnectionMessages.init(allocator);
        errdefer connection_messages.deinit();

        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, config.message_pool_capacity);
        errdefer message_pool.deinit();

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

        self.transactions.deinit();
        self.connection_messages.deinit();
        self.message_pool.deinit();

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

                    if (all_connections_closed) {
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

    pub fn tick(self: *Self) !void {
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

            try conn.tick();

            // aggregate all messages from the connection to the client inbox
            try self.gather(conn);
            try self.distribute(conn);
        }

        while (self.unprocessed_messages_queue.dequeue()) |message| {
            try self.handleMessage(message);

            // log.debug("message type {any}", .{message.headers.message_type});
            // log.debug("message ref_count {}", .{message.ref_count.load(.seq_cst)});

            if (message.refs() == 0) self.message_pool.destroy(message);
        }
    }

    pub fn handleMessage(self: *Self, message: *Message) !void {
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
            .publish => {},
            .accept => {},
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
        const req = try self.message_pool.create();
        errdefer self.message_pool.destroy(req);

        req.* = Message.new();
        req.headers.message_type = .ping;
        req.headers.origin_id = conn.origin_id;
        req.setTransactionId(uuid.v7.new());
        req.ref();
        errdefer req.deref();

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
            try self.transactions.put(req.transactionId(), &chan);

            try conn.outbox.enqueue(req);
        }

        const tx = chan.receive();
        const rep = try tx;
        _ = rep;

        // log.debug("got rep.headers {any}", .{rep.headers});

        // rep.deref();
        // if (rep.ref_count.load(.seq_cst) == 0) self.message_pool.release(rep);

        // log.debug("message pool free messages {}", .{self.message_pool.available()});
    }

    pub fn publish(self: *Self, conn: *Connection, topic_name: []const u8, body: []const u8, options: PublishOptions) !void {
        _ = options;

        const publish_message = try self.message_pool.create();
        errdefer self.message_pool.destroy(publish_message);

        publish_message.* = Message.new();

        publish_message.ref();
        errdefer publish_message.deref();

        publish_message.headers.message_type = .publish;
        publish_message.headers.origin_id = conn.origin_id;
        publish_message.setTopicName(topic_name);
        publish_message.setBody(body);

        try conn.outbox.enqueue(publish_message);
    }

    pub fn gather(self: *Self, conn: *Connection) !void {
        self.unprocessed_messages_queue.concatenate(&conn.inbox);
        conn.inbox.reset();
    }

    pub fn distribute(self: *Self, conn: *Connection) !void {
        _ = self;
        _ = conn;
        // self.mutex.lock();
        // defer self.mutex.unlock();
        //
        // // NOTE: This entry is eventually cleaned up when a connection closes. This avoids
        // // reallocations for not only updating the connection_messages map but also intializing
        // // the messages list
        // if (self.connection_messages.map.get(conn.origin_id)) |messages_list| {
        //     if (messages_list.items.len == 0) return;
        //     const available = conn.outbox.capacity - conn.outbox.count;
        //
        //     if (available == 0) return;
        //
        //     var added: u32 = 0;
        //
        //     for (0..available) |i| {
        //         if (i >= messages_list.items.len) break;
        //
        //         // FIX: we should handle the case here because we might send
        //         // duplicate messages to the client
        //         try conn.outbox.enqueue(messages_list.items[i]);
        //         added += 1;
        //     }
        //
        //     // take however many messages were added and remove them from the list
        //     std.mem.copyForwards(*Message, messages_list.items, messages_list.items[added..]);
        //     messages_list.items.len -= added;
        // }
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
