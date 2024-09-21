const std = @import("std");
const assert = std.debug.assert;

const Connection = @import("./connection.zig").Connection;
const Context = @import("./context.zig").Context;
const Mailbox = @import("mailbox.zig").Mailbox;
const Message = @import("message.zig").Message;
const MessageType = @import("message.zig").MessageType;
const ProtocolError = @import("./errors.zig").ProtocolError;

pub const ClientConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",
    /// tcp connections are accepted on this port
    port: u16 = 4000,
};

pub const Client = struct {
    const Self = @This();

    /// configuration struct of the client
    config: ClientConfig,

    // TODO: figure out what this allocator is for
    allocator: std.mem.Allocator,

    // centralized message bus used to handle all incoming and outgoing messages
    inbox: Mailbox(Message),

    // centralized message bus used to handle all incoming and outgoing messages
    outbox: Mailbox(Message),

    // TODO: there should be a pool of connections to handle high traffic
    connection: *Connection = undefined,

    // Internal state bools
    connected: bool = false,

    /// Initialize the client to connect to a node
    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) Self {
        assert(config.host.len > 0);
        assert(config.port > 0);
        assert(config.port < 65535);

        return Self{
            .config = config,
            .inbox = Mailbox(Message).init(allocator),
            .outbox = Mailbox(Message).init(allocator),
            .allocator = allocator,
            .connected = false,
            .connection = undefined,
        };
    }

    /// Deinitialize the client. Can safely be called multiple times.
    pub fn deinit(self: *Self) void {
        if (self.connected) {
            std.log.debug("client is connected, cannot deinit", .{});
            return;
        }

        self.inbox.deinit();
        self.outbox.deinit();
    }

    pub fn close(self: *Self) void {
        if (self.connected) {
            self.connection.close();

            self.connected = false;
        }
    }

    /// Connect to a node that is listening for connections.
    pub fn connect(self: *Self) !void {
        // ensure that the config is valid
        assert(self.config.host.len > 0);
        assert(self.config.port > 0);
        assert(self.config.port < 65535);

        if (self.connected) return ProtocolError.AlreadyConnected;

        // TODO: figure out if the connection should be tcp or other

        const addr = try std.net.Address.parseIp(self.config.host, self.config.port);
        const stream = try std.net.tcpConnectToAddress(addr);

        self.connected = true;
        std.log.debug("connected to {s}:{d}", .{ self.config.host, self.config.port });

        var connection = Connection.new(stream, &self.inbox, &self.outbox);
        self.connection = &connection;

        // spawn the thread to run the connection
        const th = std.Thread.spawn(.{}, Connection.run, .{&connection}) catch unreachable;
        th.detach();

        // sleep for a bit for the thread to spin up
        std.time.sleep(50 * std.time.ns_per_ms);

        var connect_request = Message.new();
        connect_request.header.id = 0;
        connect_request.header.message_type = .Connect;

        // FIX: const reply = try self.request(connect_request);
        try self.connection.send(connect_request);

        // sleep while we send message
        std.time.sleep(50 * std.time.ns_per_ms);
        // TODO: there should be a receive method here that listens for a new message
    }

    /// send a ping message to the connected node
    pub fn ping(self: *Self) !void {
        assert(self.connected);

        var ping_msg = Message.new();
        ping_msg.header.id = 123;
        ping_msg.header.message_type = .Ping;

        std.log.debug("sending ping!", .{});
        // try self.request(ping_msg);
        try self.connection.send(ping_msg);

        // sleep while we send message

        std.time.sleep(std.time.ns_per_s * 1);

        // TODO:
        //   1. construct a ping message
        //   2. create a transaction
        //   3. send ping message
        //   4. receive pong message
    }

    // TODO: this should return a protocol error
    pub fn request(self: *Self) !Message {
        _ = self;
    }

    /// Subscribe to a topic and perform an action on the message received
    // pub fn subscribe(self: *Self, ctx: *Context, topic: []const u8, callback: SubscribeHandler) !void {
    //     // ensure that the client is connected
    //     assert(self.connected);
    //
    //     _ = ctx;
    //     _ = topic;
    //     _ = callback;
    //
    //     // when a node subscribes to a topic it happens locally to the node.
    // }

    /// publish a message to a topic which is routed to all subscribers to the topic
    pub fn publish(self: *Self, ctx: *Context, message: Message) !void {
        // ensure that the client is connected
        assert(self.connected);
        // ensure that the message being published is the correct structure
        assert(message.header.message_type == MessageType.Publish);
        _ = ctx;
    }
};
