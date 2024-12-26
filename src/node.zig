const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Node);

const uuid = @import("uuid");

const constants = @import("./constants.zig");
const Message = @import("./message.zig").Message;
const MessageBus = @import("./message_bus.zig").MessageBus;
const MessagePool = @import("./message_pool.zig").MessagePool;
const Connection = @import("./connection.zig").Connection;
const ProtocolError = @import("./errors.zig").ProtocolError;
const IO = @import("./io.zig").IO;

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
    /// id of the node
    id: uuid.Uuid,

    allocator: std.mem.Allocator,

    running: bool,

    /// configuration file used by the node
    config: NodeConfig,

    /// socket used to accept new connections to the node
    listener_socket: posix.socket_t,

    /// shared reference to IO
    io: *IO,

    /// shared message pool
    message_pool: *MessagePool,

    /// Centralized message bus
    message_bus: *MessageBus,

    /// Trackers to accept new connections
    accept_submitted: bool,
    accept_completion: *IO.Completion,

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Self {
        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .running = false,
            .config = config,
            .listener_socket = undefined,
            .io = undefined,
            .message_pool = undefined,
            .message_bus = undefined,
            .accept_submitted = false,
            .accept_completion = accept_completion,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.accept_completion);
    }

    pub fn run(self: *Self) !void {
        if (self.running) @panic("already running");
        self.running = true;
        defer self.running = false;

        var io = try IO.init(8, 0);
        defer io.deinit();

        var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = message_pool_gpa.deinit();
        const message_pool_allocator = message_pool_gpa.allocator();

        var message_pool = try MessagePool.init(message_pool_allocator, constants.message_pool_max_size);
        defer message_pool.deinit();

        var message_bus_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = message_bus_gpa.deinit();
        const message_bus_allocator = message_bus_gpa.allocator();

        var message_bus = try MessageBus.init(message_bus_allocator, &io, &message_pool);
        defer message_bus.deinit();

        // initialize the listener socket
        const address = try std.net.Address.parseIp(self.config.host, self.config.port);
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;
        const listener_socket = try posix.socket(address.any.family, socket_type, protocol);
        // ensure the socket gets closed
        defer posix.close(listener_socket);

        // add options to listener socket
        try posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.bind(listener_socket, &address.any, address.getOsSockLen());
        try posix.listen(listener_socket, 128);
        log.info("listening on {s}:{d}", .{ self.config.host, self.config.port });

        // NOTE: set everything that is required for running!
        self.io = &io;
        self.message_pool = &message_pool;
        self.message_bus = &message_bus;
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
            try self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms);
        }
    }

    fn tick(self: *Self) !void {
        if (!self.accept_submitted) {
            self.io.accept(*Node, self, Node.onAccept, self.accept_completion, self.listener_socket);
            self.accept_submitted = true;
        }

        try self.message_bus.tick();
    }

    pub fn onAccept(self: *Self, comp: *IO.Completion, res: IO.AcceptError!posix.socket_t) void {
        _ = comp;

        // FIX: Should handle the case where we are unable to accept the socket
        const socket = res catch |err| {
            log.err("could not accept connection {any}", .{err});
            unreachable;
        };

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
