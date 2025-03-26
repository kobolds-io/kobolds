const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const net = std.net;
const log = std.log.scoped(.Acceptor);

const testing = std.testing;

const constants = @import("../constants.zig");
const IO = @import("../io.zig").IO;
const Channel = @import("../data_structures/channel.zig").Channel;

const State = enum {
    running,
    close,
    closed,
};

const AcceptorConfig = struct {
    host: []const u8,
    port: u16,
};

/// A standalone struct responsible for accepting connections. When a connection is accepted it is added to the `sockets`
/// ArrayList. This list can then be consumed by another thread to create proper `Connection`s.
pub const Acceptor = struct {
    const Self = @This();

    accept_completion: *IO.Completion,
    accept_submitted: bool,
    cancel_completion: *IO.Completion,
    cancel_submitted: bool,
    allocator: std.mem.Allocator,
    config: AcceptorConfig,
    io: *IO,
    mutex: std.Thread.Mutex,
    sockets: std.ArrayList(posix.socket_t),
    state: State,

    pub fn init(allocator: std.mem.Allocator, config: AcceptorConfig) !Self {
        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const accept_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(accept_completion);

        const cancel_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(cancel_completion);

        return Self{
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
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.sockets.items) |socket| {
            self.io.close_socket(socket);
        }

        self.sockets.deinit();
        self.io.deinit();
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.accept_completion);
        self.allocator.destroy(self.cancel_completion);
    }

    pub fn run(self: *Self) void {
        assert(self.state != .running);

        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch |err| {
            log.err("could not parse ip {any}", .{err});
            @panic("acceptor failed to run");
        };
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;

        const listener_socket = posix.socket(address.any.family, socket_type, protocol) catch |err| {
            log.err("unable to listen for connections on {s}:{d}: {any}", .{ self.config.host, self.config.port, err });
            @panic("acceptor failed to run");
        };
        defer posix.close(listener_socket);

        // add options to listener socket
        posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch |err| {
            log.err("unable to setsockopt {any}", .{err});
            @panic("acceptor failed to run");
        };
        posix.bind(listener_socket, &address.any, address.getOsSockLen()) catch |err| {
            log.err("unable to bind {any}", .{err});
            @panic("acceptor failed to run");
        };
        posix.listen(listener_socket, 128) catch |err| {
            log.err("unable to listen {any}", .{err});
            @panic("acceptor failed to run");
        };

        log.info("listening for TCP connections - {s}:{d}", .{ self.config.host, self.config.port });

        self.state = .running;

        // send a message back to the caller that the thread is now up and running

        while (true) {
            switch (self.state) {
                .running => {
                    // This is effectively the tick
                    if (!self.accept_submitted) {
                        self.io.accept(*Acceptor, self, Acceptor.onAccept, self.accept_completion, listener_socket);
                        self.accept_submitted = true;
                    }
                },
                .close => {
                    self.state = .closed;
                    return;
                },
                .closed => return,
            }

            self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                log.err("unable to io.run_for_ns {any}", .{err});
                @panic("acceptor failed to run");
            };
        }
    }

    fn runLoop(self: *Self, ready_channel: *Channel(anyerror!bool)) void {
        assert(self.state != .running);

        const address = std.net.Address.parseIp(self.config.host, self.config.port) catch |err| {
            ready_channel.send(err);
            log.err("could not parse ip {any}", .{err});
            @panic("acceptor failed to run");
        };
        const socket_type: u32 = posix.SOCK.STREAM;
        const protocol = posix.IPPROTO.TCP;

        const listener_socket = posix.socket(address.any.family, socket_type, protocol) catch |err| {
            ready_channel.send(err);
            log.err("unable to listen for connections on {s}:{d}: {any}", .{ self.config.host, self.config.port, err });
            @panic("acceptor failed to run");
        };
        defer posix.close(listener_socket);

        // add options to listener socket
        posix.setsockopt(listener_socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch |err| {
            ready_channel.send(err);
            log.err("unable to setsockopt {any}", .{err});
            @panic("acceptor failed to run");
        };
        posix.bind(listener_socket, &address.any, address.getOsSockLen()) catch |err| {
            ready_channel.send(err);
            log.err("unable to bind {any}", .{err});
            @panic("acceptor failed to run");
        };
        posix.listen(listener_socket, 128) catch |err| {
            ready_channel.send(err);
            log.err("unable to listen {any}", .{err});
            @panic("acceptor failed to run");
        };

        log.info("listening for TCP connections - {s}:{d}", .{ self.config.host, self.config.port });

        self.state = .running;

        // send a message back to the caller that the thread is now up and running
        ready_channel.send(true);

        while (true) {
            switch (self.state) {
                .running => {
                    // This is effectively the tick
                    if (!self.accept_submitted) {
                        self.io.accept(*Acceptor, self, Acceptor.onAccept, self.accept_completion, listener_socket);
                        self.accept_submitted = true;
                    }
                },
                .close => {
                    self.state = .closed;
                    return;
                },
                .closed => return,
            }

            self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch |err| {
                log.err("unable to io.run_for_ns {any}", .{err});
                @panic("acceptor failed to run");
            };
        }
    }

    /// spawn a background thread that will listen for new connections
    pub fn listen(self: *Self) !void {
        assert(self.state != .running);

        // create a channel that will be used to notify this scope that the acceptor
        // is ready to accept connections
        var ready_channel = Channel(anyerror!bool).init();

        const thread = try std.Thread.spawn(.{}, Acceptor.runLoop, .{ self, &ready_channel });
        thread.detach();

        const res = ready_channel.receive();
        _ = res catch |err| {
            return err;
        };
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

        self.mutex.lock();
        defer self.mutex.unlock();

        self.sockets.append(socket) catch unreachable;
    }

    pub fn close(self: *Self) void {
        // we are in some other state
        if (self.state != .running) return;

        self.state = .close;

        // if (self.accept_submitted) {
        //     // submit an accept cancellation
        //     self.accept_completion
        // }
        //
        // give this thread `n` ms to closed before we are going to just timeout
        const closed_deadline = std.time.milliTimestamp() + 1_000;

        // block until the worker fully closes
        // while (self.state != .closed) {
        while (self.state != .closed and std.time.milliTimestamp() < closed_deadline) {
            std.time.sleep(1 * std.time.ns_per_ms);
            // log.debug("waiting worker.id {}", .{self.id});
        }

        if (self.state == .closed) return;

        log.warn("acceptor did not properly close: deadline exceeded", .{});
    }
};

test "acceptor behavior" {
    const allocator = testing.allocator;
    const config = AcceptorConfig{
        .host = "127.0.0.1",
        .port = 9876,
    };

    var acceptor = try Acceptor.init(allocator, config);
    defer acceptor.deinit();

    const th = try std.Thread.spawn(.{}, Acceptor.run, .{&acceptor});

    // sleep while the thread spins up
    std.time.sleep(50 * std.time.ns_per_ms);

    try testing.expectEqual(0, acceptor.sockets.items.len);

    // dial and connect to the address
    const stream = try net.tcpConnectToHost(allocator, config.host, config.port);
    defer stream.close();

    std.time.sleep(100 * std.time.ns_per_ms);

    try testing.expectEqual(1, acceptor.sockets.items.len);

    acceptor.close();

    th.join();
}
