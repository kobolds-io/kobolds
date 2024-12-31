const std = @import("std");
const testing = std.testing;

const Message = @import("../message.zig").Message;

pub fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        capacity: u32,
        buffer: []T,
        head: usize,
        tail: usize,
        count: u32,
        mutex: std.Thread.Mutex,

        pub fn init(allocator: std.mem.Allocator, capacity: u32) !Self {
            const buffer = try allocator.alloc(T, capacity);
            errdefer allocator.free(buffer);

            return Self{
                .allocator = allocator,
                .capacity = capacity,
                .buffer = buffer,
                .head = 0,
                .tail = 0,
                .count = 0,
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.allocator.free(self.buffer);
        }

        pub fn enqueue(self: *Self, value: T) !void {
            if (self.isFull()) {
                return error.BufferFull;
            }

            self.mutex.lock();
            defer self.mutex.unlock();

            self.buffer[self.tail] = value;
            self.tail = (self.tail + 1) % self.capacity;
            self.count += 1;
        }

        pub fn dequeue(self: *Self) ?T {
            if (self.isEmpty()) return null;

            self.mutex.lock();
            defer self.mutex.unlock();

            const value = self.buffer[self.head];
            self.head = (self.head + 1) % self.capacity;
            self.count -= 1;
            return value;
        }

        pub fn isEmpty(self: *Self) bool {
            return self.count == 0;
        }

        pub fn isFull(self: *Self) bool {
            return self.count == self.capacity;
        }
    };
}

test "init/deinit" {
    const allocator = testing.allocator;

    var cb = try RingBuffer(u8).init(allocator, 100);
    defer cb.deinit();
}

test "enqueue/dequeue" {
    const allocator = testing.allocator;

    var cb = try RingBuffer(Message).init(allocator, 100);
    defer cb.deinit();

    try testing.expectEqual(true, cb.isEmpty());

    // enqueue 100 messages into the buffer
    for (0..cb.capacity) |i| {
        var message = Message.new();
        message.headers.origin_id = @intCast(i);
        try cb.enqueue(message);
    }

    try testing.expectEqual(true, cb.isFull());
    try testing.expectError(error.BufferFull, cb.enqueue(Message.new()));

    var j: u32 = 0;

    // remove all items from the queue
    while (cb.dequeue()) |message| : (j += 1) {
        _ = message;
    }

    // ensure that all items were dequeued
    try testing.expectEqual(j, cb.capacity);
    try testing.expectEqual(true, cb.isEmpty());

    // enqueue 100 messages into the buffer
    for (0..cb.capacity) |i| {
        var message = Message.new();
        message.headers.origin_id = @intCast(i);
        try cb.enqueue(message);
    }

    try testing.expectEqual(true, cb.isFull());
}
