const std = @import("std");
const testing = std.testing;

const Message = @import("../protocol/message.zig").Message;

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

        pub fn available(self: *Self) u32 {
            return self.capacity - self.count;
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

        /// Enqueue multiple items into the ring buffer.
        /// Returns the number of items actually enqueued.
        pub fn enqueueMany(self: *Self, values: []const T) u32 {
            self.mutex.lock();
            defer self.mutex.unlock();

            var added_count: u32 = 0;
            for (values) |value| {
                if (self.isFull()) break;

                self.buffer[self.tail] = value;
                self.tail = (self.tail + 1) % self.capacity;
                self.count += 1;
                added_count += 1;
            }

            return added_count;
        }

        /// Dequeue multiple items from the ring buffer.
        /// Returns the number of items actually dequeued.
        pub fn dequeueMany(self: *Self, out: []T) u32 {
            self.mutex.lock();
            defer self.mutex.unlock();

            var removed_count: u32 = 0;
            for (out) |*slot| {
                if (self.isEmpty()) break;

                slot.* = self.buffer[self.head];
                self.head = (self.head + 1) % self.capacity;
                self.count -= 1;
                removed_count += 1;
            }

            return removed_count;
        }

        pub fn concatenate(self: *Self, other: *Self) !void {

            // TODO:
            //  1. check that self.available() >= other.count;

            if (self.available() >= other.count) {
                // add every value from other to self.

            } else {
                // TODO:
                //  2. figure out how many items could be added at a time
            }
        }

        pub fn isEmpty(self: *Self) bool {
            return self.count == 0;
        }

        pub fn isFull(self: *Self) bool {
            return self.count == self.capacity;
        }

        /// unsafely reset the ring buffer to simply drop all items within
        pub fn reset(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.head = 0;
            self.tail = 0;
            self.count = 0;
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
