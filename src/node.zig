const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);

const uuid = @import("uuid");

const Message = @import("./message.zig").Message;
const MessageBus = @import("message_bus.zig").MessageBus;
const MessagePool = @import("message_pool.zig").MessagePool;
const Connection = @import("connection.zig").Connection;
const ProtocolError = @import("./errors.zig").ProtocolError;
const IO = @import("./io.zig").IO;

const constants = @import("./constants.zig");

/// static configuration used to configure the node
pub const NodeConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",
    /// tcp connections are accepted on this port
    port: u16 = 4000,
};

/// A node is the central building block for communicating between nodes
/// on either the same or different hosts.
pub const Node = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    running: bool,

    listener_socket: posix.socket_t,

    /// configuration file used by the node
    config: NodeConfig,

    /// shared reference to IO
    io: *IO,

    /// Trackers to accept new connections
    accept_submitted: bool,
    accept_completion: *IO.Completion,

    /// central message bus for all messages across all threads to post messages
    message_bus: *MessageBus,

    /// central message bus for all messages across all threads to post messages
    message_pool: *MessagePool,

    /// id of the node
    id: uuid.Uuid,

    pub fn init(
        allocator: std.mem.Allocator,
        config: NodeConfig,
        io: *IO,
        message_bus: *MessageBus,
        message_pool: *MessagePool,
    ) !Self {
        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .running = false,
            .listener_socket = undefined,
            .config = config,
            .io = io,
            .accept_submitted = false,
            .accept_completion = accept_completion,
            .message_bus = message_bus,
            .message_pool = message_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.accept_completion);
    }

    pub fn run(self: *Self) !void {
        if (self.running) @panic("already running");
        self.running = true;
        defer self.running = false;

        const address = try std.net.Address.parseIp(self.config.host, self.config.port);

        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const listener_socket = try posix.socket(address.any.family, socket_type, protocol);
        defer posix.close(listener_socket);

        try posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.bind(listener_socket, &address.any, address.getOsSockLen());
        try posix.listen(listener_socket, 128);
        log.info("listening on {s}:{d}", .{ self.config.host, self.config.port });

        self.listener_socket = listener_socket;

        var timer = try std.time.Timer.start();
        defer timer.reset();
        var start: u64 = 0;
        var printed = false;

        while (true) {
            if (start == 0 and self.message_bus.processed_messages_count >= 10) {
                timer.reset();
                start = timer.read();
            }
            if (self.message_bus.processed_messages_count >= 10) {
                const now = timer.read();
                const elapsed_seconds = (now - start) / std.time.ns_per_s;

                if (elapsed_seconds % 5 == 0) {
                    if (!printed) {
                        log.debug("processed {} messages in {}s", .{ self.message_bus.processed_messages_count, elapsed_seconds });
                        printed = true;
                    }
                } else {
                    printed = false;
                }
            }

            try self.tick();
            // if (self.message_bus.connections.count() > 1) break;
            try self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms);
        }
    }

    // FIX: We should use this kind of runner func instead since we can create/destroy everything better
    /// Alternative to run where the node is just managing the message bus and pool
    // pub fn run2(self: *Self) !void {
    //     if (self.running) @panic("already running");
    //     self.running = true;
    //     defer self.running = false;
    //
    //     var io = try IO.init(8, 0);
    //     defer io.deinit();
    //
    //     var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    //     defer _ = message_pool_gpa.deinit();
    //     const message_pool_allocator = message_pool_gpa.allocator();
    //
    //     var message_pool = try MessagePool.init(message_pool_allocator);
    //     defer message_pool.deinit();
    //
    //     var message_bus_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    //     defer _ = message_bus_gpa.deinit();
    //     const message_bus_allocator = message_bus_gpa.allocator();
    //
    //     var message_bus = try MessageBus.init(message_bus_allocator, &io, &message_pool);
    //     defer message_bus.deinit();
    //
    //     // initialize the listener socket
    //     const address = try std.net.Address.parseIp(self.config.host, self.config.port);
    //
    //     const socket_type: u32 = posix.SOCK.STREAM;
    //     const protocol = posix.IPPROTO.TCP;
    //     const listener_socket = try posix.socket(address.any.family, socket_type, protocol);
    //     defer posix.close(listener_socket);
    //
    //     try posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    //     try posix.bind(listener_socket, &address.any, address.getOsSockLen());
    //     try posix.listen(listener_socket, 128);
    //     log.info("listening on {s}:{d}", .{ self.config.host, self.config.port });
    //
    //     self.listener_socket = listener_socket;
    //
    //     while (true) {
    //         try self.tick();
    //         try self.io.run_for_ns(10 * std.time.ns_per_ms);
    //     }
    // }

    fn tick(self: *Self) !void {
        if (!self.accept_submitted) {
            self.io.accept(*Node, self, Node.onAccept, self.accept_completion, self.listener_socket);
            self.accept_submitted = true;

            // self.io.timeout(*Server, self, Server.on_accept_timeout, self.accept_completion, 1_000_000_000);
        }

        try self.message_bus.tick();
    }

    pub fn onAccept(self: *Self, comp: *IO.Completion, res: IO.AcceptError!posix.socket_t) void {
        // _ = self;
        _ = comp;
        const socket = res catch 0;

        self.accept_submitted = false;

        // TODO: in a multi-threaded application, we should have multiple message busses
        // can can process messages independently. But for now, we can just have a single one

        self.message_bus.addConnection(socket) catch unreachable;
    }
};

test "node does node things" {
    // const allocator = std.testing.allocator;
    //
    // var io = try IO.init(8, 0);
    // defer io.deinit();
    //
    // var message_bus = try MessageBus.init(allocator, &io);
    // defer message_bus.deinit();
    //
    // var node = try Node.init(allocator, .{}, &io, &message_bus);
    // defer node.deinit();
    //
    // try node.tick();
}
