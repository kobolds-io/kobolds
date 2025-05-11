const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const net = std.net;
const log = std.log.scoped(.Listener);

const testing = std.testing;

const constants = @import("../constants.zig");
const IO = @import("../io.zig").IO;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;
// const InboundConnectionConfig = @import("../protocol/connection3.zig").InboundConnectionConfig;
const Transport = @import("../protocol/connection3.zig").Transport;

const State = enum {
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
    transport: Transport = .tcp,
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
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),

    pub fn init(allocator: std.mem.Allocator, id: usize, config: ListenerConfig) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        const cancel_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(cancel_completion);

        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

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
            .sockets = std.ArrayList(posix.socket_t).init(allocator),
            .state = .closed,
            .close_channel = close_channel,
            .done_channel = done_channel,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.sockets.items) |socket| {
            self.io.close_socket(socket);
        }

        self.sockets.deinit();
        self.io.deinit();

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.accept_completion);
        self.allocator.destroy(self.cancel_completion);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        assert(self.state != .running);

        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch |err| {
            log.err("could not parse ip {any}", .{err});
            @panic("Listener failed to run");
        };
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;

        const listener_socket = posix.socket(address.any.family, socket_type, protocol) catch |err| {
            log.err("unable to listen for connections on {s}:{d}: {any}", .{ self.config.host, self.config.port, err });
            @panic("Listener failed to run");
        };
        defer posix.close(listener_socket);

        // add options to listener socket
        posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch |err| {
            log.err("unable to setsockopt {any}", .{err});
            @panic("Listener failed to run");
        };
        posix.bind(listener_socket, &address.any, address.getOsSockLen()) catch |err| {
            log.err("unable to bind {any}", .{err});
            @panic("Listener failed to run");
        };
        posix.listen(listener_socket, 128) catch |err| {
            log.err("unable to listen {any}", .{err});
            @panic("Listener failed to run");
        };

        log.info("listening for TCP connections - {s}:{d}", .{ self.config.host, self.config.port });

        self.state = .running;

        // send a message back to the caller that the thread is now up and running
        ready_channel.send(true);
        while (true) {
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("listener {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    // This is effectively the tick
                    if (!self.accept_submitted) {
                        self.io.accept(*Listener, self, Listener.onAccept, self.accept_completion, listener_socket);
                        self.accept_submitted = true;
                    }
                },
                .closing => {
                    self.mutex.lock();
                    defer self.mutex.unlock();

                    for (self.sockets.items) |socket| {
                        posix.close(socket);
                    }

                    // reset the array list
                    self.sockets.items.len = 0;

                    log.info("listener {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }

            self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                log.err("unable to io.run_for_ns {any}", .{err});
                @panic("Listener failed to run");
            };
        }
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

        // the config is zero'd and allowed inbound connections from any address
        const unspecified_address = net.Address.parseIp("0.0.0.0", 0) catch unreachable;

        if (self.config.allowed_inbound_connection_configs) |allowed_inbound_connection_configs| {
            var allowed = false;
            for (allowed_inbound_connection_configs) |allowed_inbound_connection_config| {
                const addr = net.Address.parseIp(
                    allowed_inbound_connection_config.host,
                    allowed_inbound_connection_config.port,
                ) catch |err| {
                    log.err("could not part allowed_inbound_connection address {any}", .{err});
                    continue;
                };

                const inbound_address_port = inbound_address.getPort();

                // if this config is an unspecified_address
                if (addr.in.sa.addr == unspecified_address.in.sa.addr) {
                    if (inbound_address_port < allowed_inbound_connection_config.port_min) continue;
                    if (inbound_address_port > allowed_inbound_connection_config.port_max) continue;
                    if (allowed_inbound_connection_config.port != 0 and inbound_address_port != allowed_inbound_connection_config.port) continue;

                    log.info("inbound connection from {any} is allowed by unspecified_address {}", .{
                        inbound_address,
                        unspecified_address,
                    });
                    allowed = true;
                    break;
                }

                if (inbound_address.in.sa.addr == addr.in.sa.addr) {
                    if (inbound_address_port < allowed_inbound_connection_config.port_min) continue;
                    if (inbound_address_port > allowed_inbound_connection_config.port_max) continue;
                    if (allowed_inbound_connection_config.port != 0 and inbound_address_port != allowed_inbound_connection_config.port) continue;

                    log.info("inbound connection from {any} is allowed", .{inbound_address});
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

        self.mutex.lock();
        defer self.mutex.unlock();

        self.sockets.append(socket) catch unreachable;
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }
};

test "listener behavior" {
    const allocator = testing.allocator;
    const config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 9890,
    };

    var listener = try Listener.init(allocator, 0, config);
    defer listener.deinit();

    var ready_channel = UnbufferedChannel(bool).new();
    const th = try std.Thread.spawn(.{}, Listener.run, .{ &listener, &ready_channel });

    _ = ready_channel.receive();

    try testing.expectEqual(0, listener.sockets.items.len);

    // dial and connect to the address
    const stream = try net.tcpConnectToHost(allocator, config.host, config.port);
    defer stream.close();

    std.time.sleep(100 * std.time.ns_per_ms);

    try testing.expectEqual(1, listener.sockets.items.len);

    listener.close();

    th.join();
}
