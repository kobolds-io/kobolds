const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);

const UnbufferedChannel = @import("stdx").UnbufferedChannel;

pub const ClientConfig = struct {
    max_connections: u32 = 5,
    host: []const u8 = "127.0.0.1",
    port: u16 = 8000,
};

const ClientState = enum {
    running,
    closing,
    closed,
};

pub const Client = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: ClientConfig,
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    state: ClientState,

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        return Self{
            .allocator = allocator,
            .config = config,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn connect(self: *Self) !void {
        _ = self;
    }

    pub fn disconnect(self: *Self) void {
        _ = self;
    }

    pub fn tick(self: *Self) !void {
        _ = self;
    }

    pub fn run(self: *Self) !void {
        var ready_channel = UnbufferedChannel(bool).new();
        const run_loop_thread = try std.Thread.spawn(.{}, Client.runLoop, .{ self, &ready_channel });
        run_loop_thread.detach();

        _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("run_loop_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                // block until the message is received by the background thread
                self.close_channel.send(true);
            },
        }

        // Block until the client transitions into the closed state
        _ = self.done_channel.receive();
    }

    fn runLoop(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("client running", .{});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(1 * std.time.ns_per_us) catch false;
            if (close_channel_received) {
                log.info("client closing", .{});
                self.state = .closing;
            }
            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                },
                .closing => {
                    log.info("client closed", .{});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                else => {
                    @panic("unable to tick closed client");
                },
            }
        }
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();
}

test "the api" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.run();
    defer client.close();

    try client.connect();
    defer client.disconnect();
}
