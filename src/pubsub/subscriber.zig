const std = @import("std");
const log = std.log.scoped(.Subscriber);
const assert = std.debug.assert;

const uuid = @import("uuid");
const utils = @import("../utils.zig");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const Topic = @import("./topic.zig").Topic;
const Message = @import("../protocol/message.zig").Message;

pub const Subscriber = struct {
    const Self = @This();
    pub const SubscriptionCallback = *const fn (event: Topic.TopicEvent, context: ?*anyopaque, message: *Message) void;

    allocator: std.mem.Allocator,
    conn_id: uuid.Uuid,
    mutex: std.Thread.Mutex,
    queue: *RingBuffer(*Message),
    topic: *Topic,
    key: u128, // NOTE: a unique made of the topic_name and conn_id
    callback: SubscriptionCallback,
    messages_received: u128,

    pub fn init(allocator: std.mem.Allocator, conn_id: uuid.Uuid, topic: *Topic) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, constants.subscriber_max_queue_capacity);
        errdefer queue.deinit();

        const key = utils.generateKey(topic.topic_name, conn_id);

        return Self{
            .allocator = allocator,
            .conn_id = conn_id,
            .mutex = std.Thread.Mutex{},
            .queue = queue,
            .topic = topic,
            .key = key,
            .messages_received = 0,
            .callback = Subscriber.defaultCallback,
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.queue.dequeue()) |message| {
            message.deref();
        }

        self.queue.deinit();
        self.allocator.destroy(self.queue);
    }

    pub fn subscribe(self: *Self) !void {
        try self.topic.subscribe(self);
    }

    pub fn unsubscribe(self: *Self) void {
        self.topic.unsubscribe(self.key);
    }

    fn defaultCallback(event: Topic.TopicEvent, context: ?*anyopaque, message: *Message) void {
        assert(context != null);
        _ = event;

        const subscriber: *Subscriber = @ptrCast(@alignCast(context.?));

        // because we are appending this message to the subscriber queue and it can be accessed
        // by multiple threads, we should lock it.
        subscriber.mutex.lock();
        defer subscriber.mutex.unlock();

        subscriber.messages_received += 1;

        message.ref();
        subscriber.queue.enqueue(message) catch |err| {
            log.err("subscriber could not enqueue message {any}", .{err});
            message.deref();
        };
    }
};
