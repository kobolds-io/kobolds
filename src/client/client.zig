const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Client);
// const posix = std.posix;

const uuid = @import("uuid");
const constants = @import("../constants.zig");

// const Worker = @import("./worker.zig").Worker;
// const Listener = @import("./listener.zig").Listener;
// const ListenerConfig = @import("./listener.zig").ListenerConfig;
// const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const IO = @import("../io.zig").IO;

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;
const Connection = @import("../protocol/connection.zig").Connection;

pub const ClientConfig = struct {
    max_connections: u16 = 1024,
    memory_pool_capacity: usize = 5_000,
};

const ClientState = enum {
    running,
    closing,
    closed,
};

pub const Client = struct {
    const Self = @This();

    id: uuid.Uuid,
    allocator: std.mem.Allocator,
    config: ClientConfig,
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    io: *IO,
    state: ClientState,
    memory_pool: *MemoryPool(Message),
    mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        return Self{
            .id = uuid.v7.new(),
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            .done_channel = done_channel,
            .io = io,
            .memory_pool = memory_pool,
            .state = .closed,
            .mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iterator = self.connections.valueIterator();
        while (connections_iterator.next()) |entry| {
            const connection = entry.*;

            assert(connection.state == .closed);

            connection.deinit();
            self.allocator.destroy(connection);
        }

        self.io.deinit();
        self.memory_pool.deinit();

        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.close_channel);
    }

    pub fn start(self: *Self) !void {
        // Start the core thread
        var ready_channel = UnbufferedChannel(bool).new();
        const client_thread = try std.Thread.spawn(.{}, Client.run, .{ self, &ready_channel });
        client_thread.detach();

        _ = ready_channel.timedReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("client_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn run(self: *Client, ready_channel: *UnbufferedChannel(bool)) void {
        self.state = .running;
        ready_channel.send(true);
        log.info("client {} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("client {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;

                    // FIX: this should us io uring or something much nicer
                    // std.time.sleep(100 * std.time.ns_per_ms);
                },
                .closing => {
                    log.info("client {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                self.mutex.lock();
                defer self.mutex.unlock();

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
    }

    fn tick(self: *Self) !void {
        _ = self;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{});
    defer client.deinit();

    try client.start();
    defer client.close();
}
