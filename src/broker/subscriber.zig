const std = @import("std");
const utils = @import("../utils.zig");
const uuid = @import("uuid");

const RingBuffer = @import("stdx").RingBuffer;
const Envelope = @import("./envelope.zig").Envelope;

pub const Subscriber = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    session_id: u64,
    key: u64,
    queue: *RingBuffer(Envelope),

    pub fn init(allocator: std.mem.Allocator, key: u64, session_id: u64, queue_capacity: usize) !Self {
        const queue = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(Envelope).init(allocator, queue_capacity);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .session_id = session_id,
            .key = key,
            .queue = queue,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();

        self.allocator.destroy(self.queue);
    }
};
