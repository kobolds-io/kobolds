const std = @import("std");
const uuid = @import("uuid");

const RingBuffer = @import("stdx").RingBuffer;

pub fn Producer(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        conn_id: uuid.Uuid,
        id: uuid.Uuid,
        mutex: std.Thread.Mutex,
        produced_count: u128,
        queue: *RingBuffer(T),

        pub fn init(allocator: std.mem.Allocator, id: uuid.Uuid, conn_id: uuid.Uuid, queue_capacity: usize) !Self {
            const queue = try allocator.create(RingBuffer(T));
            errdefer allocator.destroy(queue);

            queue.* = try RingBuffer(T).init(allocator, queue_capacity);
            errdefer queue.deinit();

            return Self{
                .allocator = allocator,
                .conn_id = conn_id,
                .id = id,
                .mutex = .{},
                .produced_count = 0,
                .queue = queue,
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
            self.allocator.destroy(self.queue);
        }

        pub fn produce(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            try self.queue.enqueue(value);

            self.produced_count += 1;
        }
    };
}
