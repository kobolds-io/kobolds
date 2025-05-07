const std = @import("std");
const utils = @import("../utils.zig");
const uuid = @import("uuid");

const UnbufferedChannel = @import("stdx").UnbufferedChannel;
const RingBuffer = @import("stdx").RingBuffer;

pub fn Consumer(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        consumed_count: u128,
        conn_id: uuid.Uuid,
        id: uuid.Uuid,
        mutex: std.Thread.Mutex,
        queue: *RingBuffer(T),

        // a worker needs to be able to tie a consumer to a connection so this needs a connection id

        pub fn init(allocator: std.mem.Allocator, id: u128, conn_id: uuid.Uuid, queue_capacity: usize) !Self {
            const queue = try allocator.create(RingBuffer(T));
            errdefer allocator.destroy(queue);

            queue.* = try RingBuffer(T).init(allocator, queue_capacity);
            errdefer queue.deinit();

            return Self{
                .allocator = allocator,
                .consumed_count = 0,
                .conn_id = conn_id,
                .id = id,
                .mutex = .{},
                .queue = queue,
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();

            self.allocator.destroy(self.queue);
        }

        pub fn tick(self: *Self) !void {
            if (self.queue.count == 0) return;

            self.mutex.lock();
            defer self.mutex.unlock();

            // FIX: consumer should do something
            self.consumed_count += self.queue.count;
            self.queue.reset();
        }
    };
}
