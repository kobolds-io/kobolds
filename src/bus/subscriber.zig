const std = @import("std");
const utils = @import("../utils.zig");
const uuid = @import("uuid");

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const RingBuffer = @import("stdx").RingBuffer;
const Bus = @import("./bus.zig").Bus;
const Message = @import("../protocol/message.zig").Message;

pub const Subscriber = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    consumed_count: u128,
    conn_id: uuid.Uuid,
    key: u128,
    mutex: std.Thread.Mutex,
    queue: *RingBuffer(*Message),
    bus: *Bus,

    // a worker needs to be able to tie a consumer to a connection so this needs a connection id

    pub fn init(
        allocator: std.mem.Allocator,
        key: u128,
        conn_id: uuid.Uuid,
        queue_capacity: usize,
        bus: *Bus,
    ) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, queue_capacity);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .consumed_count = 0,
            .conn_id = conn_id,
            .key = key,
            .mutex = .{},
            .queue = queue,
            .bus = bus,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();

        self.allocator.destroy(self.queue);
    }

    pub fn subscribe(self: *Self) !void {
        try self.bus.addSubscriber(self);
    }

    pub fn unsubscribe(self: *Self) !void {
        _ = try self.bus.removeSubscriber(self.key);
    }
};
