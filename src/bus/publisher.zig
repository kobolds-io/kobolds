const std = @import("std");
const uuid = @import("uuid");

const RingBuffer = @import("stdx").RingBuffer;
const Message = @import("../protocol/message.zig").Message;
const Bus = @import("./bus.zig").Bus;

pub const Publisher = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    conn_id: uuid.Uuid,
    key: u128,
    mutex: std.Thread.Mutex,
    published_count: u128,
    queue: *RingBuffer(*Message),
    topic_name: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        key: u128,
        conn_id: uuid.Uuid,
        queue_capacity: usize,
        topic_name: []const u8,
    ) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, queue_capacity);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .conn_id = conn_id,
            .key = key,
            .mutex = .{},
            .published_count = 0,
            .queue = queue,
            .topic_name = topic_name,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();
        self.allocator.destroy(self.queue);
    }

    pub fn publish(self: *Self, message: *Message) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.queue.enqueue(message);

        self.published_count += 1;
    }
};
