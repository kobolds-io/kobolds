const std = @import("std");
const testing = std.testing;
const uuid = @import("uuid");

const Message = @import("../protocol/message.zig").Message;
const EventEmitter = @import("../data_structures/event_emitter.zig").EventEmitter;

const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;
// const UnmanagedQueue = @import("../data_structures/unmanaged_queue.zig").UnmanagedQueue;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;

pub const TopicEvent = enum {
    publish,
};

// this kind of topic is meant to publish messages TO subscribers. It only really works for very fast subscribers.
// It does not allow for a subscriber to be slower than any other subscriber.
pub const Topic = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    ee: EventEmitter(TopicEvent, *Message),
    mutex: std.Thread.Mutex,
    queue: MessageQueue,
    topic_name: []const u8,

    pub fn init(allocator: std.mem.Allocator, topic_name: []const u8) !Self {
        const t_name = try allocator.dupe(u8, topic_name);
        errdefer allocator.free(t_name);

        return Self{
            .allocator = allocator,
            .ee = EventEmitter(TopicEvent, *Message).init(allocator),
            .mutex = std.Thread.Mutex{},
            .queue = MessageQueue.new(),
            .topic_name = t_name,
        };
    }

    pub fn deinit(self: *Self) void {
        // self.queue.deinit();
        self.ee.deinit();
        // self.allocator.destroy(self.queue);
        self.allocator.free(self.topic_name);
    }

    pub fn publish(self: *Self) !void {
        if (self.queue.dequeue()) |message| {
            message.deref();
            self.ee.emit(.publish, message);
        } else {
            return error.QueueEmpty;
        }
    }

    pub fn subscribe(
        self: *Self,
        callback: EventEmitter(TopicEvent, *Message).ListenerCallback,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.ee.addEventListener(.publish, callback);
    }

    pub fn unsubscribe(
        self: *Self,
        callback: EventEmitter(TopicEvent, *Message).ListenerCallback,
    ) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.ee.removeEventListener(.publish, callback);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var message = Message.new();
    message.headers.message_type = .publish;
    message.setTopicName("test");

    var topic = try Topic.init(allocator, "hello_world");
    defer topic.deinit();

    topic.queue.enqueue(&message);

    try testing.expectEqual(1, topic.queue.count);
}

var test_ring_buffer: RingBuffer(*Message) = undefined;

test "pubsub" {
    const allocator = testing.allocator;

    test_ring_buffer = try RingBuffer(*Message).init(allocator, 10);
    defer test_ring_buffer.deinit();

    const sub_1_cb = struct {
        pub fn cb(event: TopicEvent, data: *Message) void {
            _ = event;

            data.ref();
            test_ring_buffer.enqueue(data) catch unreachable;
        }
    }.cb;

    const sub_2_cb = struct {
        pub fn cb(event: TopicEvent, data: *Message) void {
            _ = event;

            data.ref();
            test_ring_buffer.enqueue(data) catch unreachable;
        }
    }.cb;

    var topic = try Topic.init(allocator, "hello_world");
    defer topic.deinit();

    try topic.subscribe(sub_1_cb);
    defer _ = topic.unsubscribe(sub_1_cb);

    try topic.subscribe(sub_2_cb);
    defer _ = topic.unsubscribe(sub_2_cb);

    var message = Message.new();
    message.headers.message_type = .publish;
    message.setTopicName(topic.topic_name);
    message.ref();

    topic.queue.enqueue(&message);

    try testing.expectEqual(0, test_ring_buffer.count);
    try testing.expectEqual(1, message.refs());
    try testing.expectEqual(1, topic.queue.count);

    try topic.publish();

    try testing.expectEqual(2, message.refs());
    try testing.expectEqual(0, topic.queue.count);

    try testing.expectEqual(2, test_ring_buffer.count);
}
