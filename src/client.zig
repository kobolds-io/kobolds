const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);

const uuid = @import("uuid");

const constants = @import("constants.zig");
const Message = @import("./message.zig").Message;
const Accept = @import("./message.zig").Accept;
const Pong = @import("./message.zig").Pong;
const Ping = @import("./message.zig").Ping;

const Reqeust = @import("./message.zig").Request;
const Reply = @import("./message.zig").Reply;

const Compression = @import("./message.zig").Compression;
const MessagePool = @import("./message_pool.zig").MessagePool;
const RingBuffer = @import("./data_structures/ring_buffer.zig").RingBuffer;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;
const Connection = @import("./connection.zig").Connection;
const ProtocolError = @import("./errors.zig").ProtocolError;
const IO = @import("./io.zig").IO;

pub const ClientConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",

    /// tcp connections are accepted on this port
    port: u16 = 8000,

    /// default compression applied to the bodies of messages
    compression: Compression = .none,
};

pub const Client = struct {
    const Self = @This();

    /// configuration struct of the client
    config: ClientConfig,
    allocator: std.mem.Allocator,
    io: *IO,
    message_pool: *MessagePool,

    connection: *Connection = undefined,
    socket: posix.socket_t,

    /// Trackers to accept new connections
    connect_submitted: bool,
    connect_buf: [constants.connection_recv_buffer_size]u8,
    connect_completion: *IO.Completion,

    // Internal state bools
    connected: bool,
    accepted: bool,

    inbox: *MessageQueue,
    outbox: *RingBuffer(*Message),

    loop_thread: std.Thread,
    shutdown: bool,

    pub fn init(allocator: std.mem.Allocator, io: *IO, message_pool: *MessagePool, config: ClientConfig) !Self {
        const connect_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(connect_completion);

        // create the inbox and outbox for the connection
        const inbox = try allocator.create(MessageQueue);
        errdefer allocator.destroy(inbox);

        const outbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(outbox);

        inbox.* = MessageQueue.new(constants.message_queue_capacity_default);

        outbox.* = RingBuffer(*Message).init(allocator, 5_000) catch unreachable;
        errdefer outbox.deinit();

        return Self{
            .allocator = allocator,
            .io = io,
            .message_pool = message_pool,
            .config = config,
            .connected = false,
            .accepted = false,
            .socket = undefined,
            .connect_completion = connect_completion,
            .connect_submitted = false,
            .connect_buf = undefined,
            .loop_thread = undefined,
            .inbox = inbox,
            .outbox = outbox,
            .shutdown = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.connect_completion);

        if (self.connected) {
            self.connected = false;
            self.connection.deinit();
            posix.close(self.socket);
            self.allocator.destroy(self.connection);
        }

        self.inbox.clear();
        self.outbox.deinit();

        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);

        self.shutdown = true;
    }

    /// Connect to a node
    pub fn connect(self: *Self) !void {
        // ensure that the client is not already connected
        assert(!self.connected);
        assert(!self.connect_submitted);

        // ensure that the config is valid
        assert(self.config.host.len > 0);
        assert(self.config.port > 0 and self.config.port < std.math.maxInt(u16));

        const address = try std.net.Address.parseIp(self.config.host, self.config.port);

        // create a client socket
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const socket = try posix.socket(address.any.family, socket_type, protocol);
        errdefer posix.close(socket);

        // capture a reference to the newly created socket
        self.socket = socket;

        // if we get here then that means that we messed up the logic somewhere;
        self.io.connect(*Client, self, Client.onConnect, self.connect_completion, socket, address);
        self.connect_submitted = true;

        log.info("connecting to {s}:{any}", .{ self.config.host, self.config.port });
    }

    pub fn run(self: *Client) !void {
        // TODO: add safety checks to ensure we don't get into a bad state
        self.shutdown = false;

        // spawn a thread in the background that will run the tick loop
        const thread = try std.Thread.spawn(.{}, Client.runLoop, .{self});
        thread.detach();

        // sleep to allow thread to spawn
        std.time.sleep(50 * std.time.ns_per_ms);
    }

    // spins down the client
    pub fn close(self: *Client) void {
        // ensure that we are in the correct state
        assert(!self.shutdown);
        self.shutdown = true;
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    pub fn runLoop(self: *Client) void {
        while (!self.shutdown) {
            self.tick() catch |err| {
                std.debug.print("client.tick err {any}\n", .{err});
                return;
            };

            // self.io.run_for_ns(1000 * std.time.ns_per_ms) catch |err| {
            self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                std.debug.print("io err {any}\n", .{err});
                return;
            };
        }
    }

    pub fn tick(self: *Self) !void {
        // check if this client is already connected

        // if the client is not connected and connect hasn't been submitted
        // then we should try to connect and break out of this loop

        // if we haven't connected AND we are awaiting accept we should tick the connection
        // and see if we have the accept message

        if (!self.connected and !self.connect_submitted) {
            try self.connect();
            // sleep before trying again
            std.time.sleep(1 * std.time.ns_per_ms);
            return;
        }

        // TODO: we should return an error
        if (self.connection.state == .closed) {
            return;
        }

        try self.connection.tick();

        while (self.connection.inbox.dequeue()) |message| {
            try self.handleMessage(message);

            // message is no longer needed
            message.deref();

            assert(message.ref_count == 0);

            if (message.ref_count == 0) {
                self.message_pool.destroy(message);
            }
        }

        // gather all the messages in the connection inbox and add them to the the processing queue

    }

    pub fn handleMessage(self: *Client, message: *Message) !void {
        // log.info("received {any} message", .{message.headers.message_type});
        switch (message.headers.message_type) {
            .accept => {
                // ensure that we have not already set this origin_id
                assert(self.connection.origin_id == 0);

                // cast the headers into the correct headers type
                const accept_headers: *const Accept = message.headers.intoConst(.accept).?;

                // ensure that we are not getting some messed up message
                assert(accept_headers.origin_id != accept_headers.accepted_origin_id);

                self.connection.origin_id = accept_headers.accepted_origin_id;

                self.accepted = true;

                // cast the message to the correct type
            },
            .pong => {
                // if (self.connection.messages_recv % 1_000 == 0) {
                // }
                log.debug("received pong. Total messages {}", .{self.connection.messages_recv});

                // cast the headers into the correct headers type
                // const pong_headers: *const Pong = message.headers.intoConst(.pong).?;

                // _ = pong_headers;

                // cast the message to the correct type
            },
            else => |t| {
                // this is some random message that we weren't expecting
                log.err("received unexpected message {any}", .{t});
            },
        }
    }

    pub fn ping(self: *Client) !void {
        if (!self.connected or !self.accepted) return ProtocolError.ConnectionClosed;

        // FIX: this is just a hack
        if (self.message_pool.unassigned_queue.count < 50) return;

        const message = try self.message_pool.create();
        errdefer self.message_pool.destroy(message);

        // construct a ping message
        message.* = Message.new();
        message.headers.message_type = .ping;
        message.headers.compression = self.config.compression;

        var ping_headers: *Ping = message.headers.into(.ping).?;
        ping_headers.transaction_id = uuid.v7.new();

        // compress the message according to the compression scheme
        try message.compress();

        try self.enqueue_send_message(message);
    }

    pub fn request(self: *Client, topic: []const u8, body: []const u8) !void {
        _ = topic;
        if (!self.connected or !self.accepted) return ProtocolError.ConnectionClosed;

        const message = try self.message_pool.create();
        errdefer self.message_pool.destroy(message);

        // construct a ping message
        message.* = Message.new();
        message.headers.message_type = .request;
        message.headers.compression = .gzip;

        var req_headers: *Reqeust = message.headers.into(.request).?;
        req_headers.transaction_id = uuid.v7.new();

        message.setBody(body);

        // compress the message according to the compression scheme
        try message.compress();

        try self.enqueue_send_message(message);
    }

    // TODO: this should return a Request struct
    pub fn enqueue_send_message(self: *Client, message: *Message) !void {
        if (!self.connected or !self.accepted) return ProtocolError.ConnectionClosed;

        message.headers.origin_id = self.connection.origin_id;

        // TODO: ensure that the message is a valid request message type
        // TODO: create a transaction for this request

        self.connection.outbox_mutex.lock();
        defer self.connection.outbox_mutex.unlock();

        message.ref();
        errdefer message.deref();

        try self.connection.outbox.enqueue(message);
    }

    pub fn onConnect(client: *Client, completion: *IO.Completion, result: IO.ConnectError!void) void {
        // reset the submission
        client.connect_submitted = false;

        _ = completion;
        _ = result catch |err| {
            log.err("onConnect err {any}", .{err});
        };

        const conn = client.allocator.create(Connection) catch |err| {
            // something exploded and we should ensure we cleanup
            log.err("client connection allocation step exploded {any}", .{err});
            return;
        };

        conn.* = Connection.init(
            0,
            client.io,
            client.socket,
            client.inbox,
            client.outbox,
            client.allocator,
            client.message_pool,
        );

        client.connection = conn;
        client.connected = true;

        log.info("connected {s}:{d}", .{ client.config.host, client.config.port });
    }
};

test "client does client things" {}
