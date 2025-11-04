const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const net = std.net;
const testing = std.testing;
const log = std.log.scoped(.Listener);

const constants = @import("../constants.zig");

const IO = @import("../io.zig").IO;

const stdx = @import("stdx");
const UnbufferedChannel = stdx.UnbufferedChannel;

const Transport = @import("../protocol/connection.zig").Transport;

const State = enum {
    inactive,
    running,
    closing,
    closed,
};

/// A whitelist
pub const AllowedInboundConnectionConfig = struct {
    /// when set to 0.0.0.0 allow all
    host: []const u8 = "0.0.0.0",
    port: u16 = 0,
    port_min: u16 = 0,
    port_max: u16 = std.math.maxInt(u16),
};

// TODO: a user should be able to configure how many connections they want for a particular host.
// TODO: a user should be able to provide a token/key for authentication to remotes
pub const ListenerConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 8000,
    transport: Transport = .tcp,
    /// a list of hosts that are allowed to communicate with this node. If `null`, all are allowed
    allowed_inbound_connection_configs: ?[]const AllowedInboundConnectionConfig = null,
};

/// A standalone struct responsible for accepting connections. When a connection is accepted it is added to the `sockets`
/// ArrayList. This list can then be consumed by another thread to create proper `Connection`s.
pub const Listener = struct {
    const Self = @This();

    id: usize,
    accept_completion: *IO.Completion,
    accept_submitted: bool,
    cancel_completion: *IO.Completion,
    cancel_submitted: bool,
    allocator: std.mem.Allocator,
    config: ListenerConfig,
    io: *IO,
    mutex: std.Thread.Mutex,
    sockets: std.ArrayList(posix.socket_t),
    state: State,
    listener_socket: ?posix.socket_t = null,

    pub fn init(
        allocator: std.mem.Allocator,
        io: *IO,
        id: usize,
        config: ListenerConfig,
    ) !Self {
        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        const cancel_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(cancel_completion);

        return Self{
            .id = id,
            .accept_completion = accept_completion,
            .accept_submitted = false,
            .cancel_completion = cancel_completion,
            .cancel_submitted = false,
            .allocator = allocator,
            .config = config,
            .io = io,
            .mutex = std.Thread.Mutex{},
            .sockets = .empty,
            .listener_socket = null,
            .state = .inactive,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.listener_socket) |listener_socket| {
            log.info("closing listener {d} {s}:{d}", .{
                self.id,
                self.config.host,
                self.config.port,
            });
            self.io.close_socket(listener_socket);
            self.listener_socket = null;
        }

        for (self.sockets.items) |socket| {
            self.io.close_socket(socket);
        }

        self.sockets.deinit(self.allocator);

        self.allocator.destroy(self.accept_completion);
        self.allocator.destroy(self.cancel_completion);
    }

    fn handleInitListener(self: *Self) !void {
        assert(self.state == .inactive);

        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch |err| {
            log.err("could not parse ip {any}", .{err});
            return err;
        };
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;

        const listener_socket = self.io.open_socket(
            address.any.family,
            socket_type,
            protocol,
        ) catch |err| {
            log.err("unable to create socket: {any}", .{err});
            return err;
        };
        errdefer self.io.close_socket(listener_socket);

        // add options to listener socket
        posix.setsockopt(
            listener_socket,
            posix.SOL.SOCKET,
            posix.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |err| {
            log.err("unable to setsockopt {any}", .{err});
            return err;
        };

        posix.bind(listener_socket, &address.any, address.getOsSockLen()) catch |err| {
            log.err("unable to bind {any}", .{err});
            return err;
        };

        posix.listen(listener_socket, 128) catch |err| {
            log.err("unable to listen {any}", .{err});
            return err;
        };

        self.listener_socket = listener_socket;
        self.state = .running;

        log.info("listening on {s}:{d}", .{ self.config.host, self.config.port });
    }

    pub fn tick(self: *Self) !void {
        switch (self.state) {
            .inactive => try self.handleInitListener(),
            .running => try self.handleAccept(),
            .closing => {
                if (self.listener_socket) |listener_socket| {
                    log.info("closing listener {d} {s}:{d}", .{
                        self.id,
                        self.config.host,
                        self.config.port,
                    });
                    self.io.close_socket(listener_socket);
                    self.listener_socket = null;
                }
                self.state = .closed;
                return;
            },
            .closed => return,
        }
    }

    fn handleAccept(self: *Self) !void {
        if (self.listener_socket) |listener_socket| {
            // This is effectively the tick
            if (!self.accept_submitted) {
                self.io.accept(
                    *Listener,
                    self,
                    Listener.onAccept,
                    self.accept_completion,
                    listener_socket,
                );
                self.accept_submitted = true;
            }
        } else @panic("invalid state, missing listener_socket");
    }

    // Callback functions
    pub fn onAccept(self: *Self, comp: *IO.Completion, res: IO.AcceptError!posix.socket_t) void {
        _ = comp;

        // FIX: Should handle the case where we are unable to accept the socket
        const socket = res catch |err| {
            log.err("could not accept connection {any}", .{err});
            unreachable;
        };

        self.accept_submitted = false;

        var inbound_address: net.Address = undefined;
        var inbound_address_len: posix.socklen_t = @sizeOf(posix.sockaddr);

        posix.getpeername(socket, &inbound_address.any, &inbound_address_len) catch |err| {
            log.err("cannot getpeername from inbound socket {any}", .{err});
            posix.close(socket);
        };

        // if the config is zero'd and allowed inbound connections from any address
        const unspecified_address = net.Address.parseIp("0.0.0.0", 0) catch unreachable;

        if (self.config.allowed_inbound_connection_configs) |allowed_inbound_connection_configs| {
            var allowed = false;
            for (allowed_inbound_connection_configs) |allowed_inbound_connection_config| {
                const addr = net.Address.parseIp(
                    allowed_inbound_connection_config.host,
                    allowed_inbound_connection_config.port,
                ) catch |err| {
                    log.err("could not parse allowed_inbound_connection address {any}", .{err});
                    continue;
                };

                const inbound_address_port = inbound_address.getPort();

                // if this config is an unspecified_address
                if (addr.in.sa.addr == unspecified_address.in.sa.addr) {
                    if (inbound_address_port < allowed_inbound_connection_config.port_min) continue;
                    if (inbound_address_port > allowed_inbound_connection_config.port_max) continue;
                    if (allowed_inbound_connection_config.port != 0 and inbound_address_port != allowed_inbound_connection_config.port) continue;

                    // log.info("inbound connection from {any} is allowed by unspecified_address {f}", .{
                    //     inbound_address,
                    //     unspecified_address,
                    // });
                    allowed = true;
                    break;
                }

                if (inbound_address.in.sa.addr == addr.in.sa.addr) {
                    if (inbound_address_port < allowed_inbound_connection_config.port_min) continue;
                    if (inbound_address_port > allowed_inbound_connection_config.port_max) continue;
                    if (allowed_inbound_connection_config.port != 0 and inbound_address_port != allowed_inbound_connection_config.port) continue;

                    // log.info("inbound connection from {any} is allowed", .{inbound_address});
                    allowed = true;
                    break;
                }
            }

            if (!allowed) {
                log.err("inbound_connection from {any} not allowed", .{inbound_address});
                // we are going to close this connection
                posix.close(socket);
                return;
            }
        }

        // FIX: this should come from config.socket_options
        const TCP_NODELAY = 1;
        posix.setsockopt(
            socket,
            posix.IPPROTO.TCP,
            posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, TCP_NODELAY)),
        ) catch unreachable;

        self.sockets.append(self.allocator, socket) catch unreachable;
    }
};

test "listener behavior" {
    const allocator = testing.allocator;
    const config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 9898,
    };

    var io = try IO.init(2, 0);
    defer io.deinit();

    var listener = try Listener.init(allocator, &io, 0, config);
    defer listener.deinit();

    try testing.expect(listener.listener_socket == null);
    try testing.expect(listener.state == .inactive);

    try listener.tick();

    try testing.expect(listener.listener_socket != null);
    try testing.expect(listener.state == .running);

    // intentionally close this listener
    listener.state = .closing;

    try listener.tick();

    // ensure that the listener is fully closed
    try testing.expect(listener.listener_socket == null);
    try testing.expect(listener.state == .closed);
}
