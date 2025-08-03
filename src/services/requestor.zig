const std = @import("std");
const utils = @import("../utils.zig");
const uuid = @import("uuid");

const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;

pub const Requestor = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    conn_id: uuid.Uuid,
    key: u128,
    queue: *RingBuffer(*Message),

    pub fn init(allocator: std.mem.Allocator, key: u128, conn_id: uuid.Uuid, queue_capacity: usize) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, queue_capacity);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .conn_id = conn_id,
            .key = key,
            .queue = queue,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();

        self.allocator.destroy(self.queue);
    }
};
