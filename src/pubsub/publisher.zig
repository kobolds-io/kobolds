const std = @import("std");
const uuid = @import("uuid");
const utils = @import("../utils.zig");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const Topic = @import("./topic.zig").Topic;
const Message = @import("../protocol/message.zig").Message;

pub const Publisher = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    conn_id: uuid.Uuid,
    queue: *RingBuffer(*Message),
    topic: *Topic,
    key: u128,

    pub fn init(allocator: std.mem.Allocator, conn_id: uuid.Uuid, topic: *Topic) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, constants.publisher_max_queue_capacity);
        errdefer queue.deinit();

        const key = utils.generateKey(topic.topic_name, conn_id);

        return Self{
            .allocator = allocator,
            .conn_id = conn_id,
            .queue = queue,
            .topic = topic,
            .key = key,
        };
    }

    pub fn deinit(self: *Self) void {
        while (self.queue.dequeue()) |message| {
            message.deref();
        }

        self.queue.deinit();
        self.allocator.destroy(self.queue);
    }

    pub fn publish(self: *Self, message: *Message) !void {
        try self.topic.publish(message);
    }

    pub fn publishMany(self: *Self, messages: []*Message) !void {
        try self.topic.publishMany(messages);
    }
};
