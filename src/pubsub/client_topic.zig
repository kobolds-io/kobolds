const std = @import("std");
const log = std.log.scoped(.Topic);
const testing = std.testing;
const assert = std.debug.assert;
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message2.zig").Message;

pub const TopicOptions = struct {
    queue_capacity: usize = constants.topic_max_queue_capacity,
};

pub const TopicError = error{
    TopicQueueFull,
    UnableToProcess,
};

pub const ClientTopic = struct {
    const Self = @This();

    pub const Callback = *const fn (message: *Message) void;

    allocator: std.mem.Allocator,
    memory_pool: *MemoryPool(Message),
    queue: *RingBuffer(*Message),
    callbacks: std.AutoHashMapUnmanaged(u64, Callback),
    topic_name: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        topic_name: []const u8,
        options: TopicOptions,
    ) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        // TODO: the buffer size should be configured. perhaps this could be a NodeConfig thing
        queue.* = try RingBuffer(*Message).init(allocator, options.queue_capacity);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .memory_pool = memory_pool,
            .queue = queue,
            .callbacks = .empty,
            .topic_name = topic_name,
        };
    }

    pub fn deinit(self: *Self) void {
        self.clearQueue();
        self.queue.deinit();
        self.callbacks.deinit(self.allocator);
        self.allocator.destroy(self.queue);
    }

    fn clearQueue(self: *Self) void {
        while (self.queue.dequeue()) |message| {
            assert(message.refs() == 1);
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }
    }

    pub fn tick(self: *Self) !void {
        if (self.queue.count == 0) return;
        if (self.callbacks.count() == 0) {
            self.clearQueue();
            return;
        }

        while (self.queue.dequeue()) |message| {
            defer {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            const subscriber_callbacks_iter = self.callbacks.valueIterator();
            while (subscriber_callbacks_iter.next()) |entry| {
                const callback = entry.*;
                callback(message);
            }
        }
    }

    pub fn addCallback(self: *Self, callback_id: u64, callback: Callback) !void {
        try self.callbacks.put(self.allocator, callback_id, callback);
    }

    pub fn removeCallback(self: *Self, callback_id: u64) bool {
        return self.callbacks.remove(callback_id);
    }
};
