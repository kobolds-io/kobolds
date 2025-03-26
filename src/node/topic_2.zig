const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Topic);

const uuid = @import("uuid");

const Message = @import("../protocol/message.zig").Message;
const Subscriber = @import("./subscriber.zig").Subscriber;

// Datastructures
const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;

pub const Topic = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    queue: MessageQueue,
    mutex: std.Thread.Mutex,
    topic_name: []const u8,
    subscribers: std.AutoHashMap(uuid.Uuid, *Subscriber),
    message_pool: *MessagePool,

    pub fn init(
        allocator: std.mem.Allocator,
        topic_name: []const u8,
        message_pool: *MessagePool,
    ) !Self {
        const t_name = try allocator.dupe(u8, topic_name);
        errdefer allocator.free(t_name);

        return Self{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .topic_name = t_name,
            .queue = MessageQueue.new(),
            .subscribers = std.AutoHashMap(uuid.Uuid, *Subscriber).init(allocator),
            .message_pool = message_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |subscriber_ptr| {
            const subscriber = subscriber_ptr.*;

            while (subscriber.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.message_pool.destroy(message);
            }

            subscriber.queue.deinit();
            self.allocator.destroy(subscriber);
        }

        while (self.queue.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.message_pool.destroy(message);
        }

        self.queue.reset();
        self.subscribers.deinit();
        self.allocator.free(self.topic_name);
    }

    pub fn tick(self: *Self) !void {
        if (self.queue.count > 0) {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.subscribers.count() == 0) {
                // log.warn("no subscribers for topic {s}. queue size {}", .{ self.topic_name, self.queue.count });
                while (self.queue.dequeue()) |message| {
                    message.deref();
                    if (message.refs() == 0) self.message_pool.destroy(message);
                }

                // drop all the messages in queue
                return;
            }

            const enqueable_messages = self.getEnqueableMessages();

            // there is nothing we can do on this tick
            if (enqueable_messages == 0) return;

            const buf = try self.allocator.alloc(*Message, enqueable_messages);
            defer self.allocator.free(buf);

            const n = self.queue.dequeueMany(buf);

            // ensure that we have a buffer that is full of messages
            assert(n == enqueable_messages);

            var subscribers_iter = self.subscribers.valueIterator();
            while (subscribers_iter.next()) |subscriber_ptr| {
                const subscriber = subscriber_ptr.*;

                // increment the references count for each message
                for (buf[0..n]) |message| {
                    message.ref();
                }

                const t = subscriber.queue.enqueueMany(buf[0..n]);
                try subscriber.publish();
                assert(t == n);
            }
        }

        //  NOTE: loop over each subscriber and notify them that there are messages to process???
    }

    fn getEnqueableMessages(self: *Self) usize {
        var enqueable_messages: usize = 0;

        // get the maximum number of messages that can be appended to every subscriber
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |subscriber_ptr| {
            const subscriber = subscriber_ptr.*;
            const queue_available = subscriber.queue.available();
            if (enqueable_messages == 0) {
                enqueable_messages = queue_available;
                continue;
            }

            if (enqueable_messages < queue_available) {
                enqueable_messages = queue_available;
            }
        }

        if (self.queue.count < enqueable_messages) {
            enqueable_messages = self.queue.count;
        }

        return enqueable_messages;
    }

    pub fn subscribe(self: *Self, id: uuid.Uuid) !*Subscriber {
        self.mutex.lock();
        defer self.mutex.unlock();

        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = try Subscriber.init(self.allocator, id);
        errdefer subscriber.deinit();

        try self.subscribers.put(id, subscriber);

        return subscriber;
    }

    pub fn unsubscribe(self: *Self, id: uuid.Uuid) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.subscribers.get(id)) |subscriber| {
            while (subscriber.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.message_pool.destroy(message);
            }

            subscriber.deinit();
            self.allocator.destroy(subscriber);

            _ = self.subscribers.remove(id);
        }
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 100);
    defer message_pool.deinit();

    var topic = try Topic.init(allocator, "hello", &message_pool);
    defer topic.deinit();
}

test "functionality" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 100);
    defer message_pool.deinit();

    var message_1 = try message_pool.create();
    defer message_pool.destroy(message_1);

    message_1.* = Message.new();
    message_1.headers.message_type = .publish;
    message_1.setTopicName("hello");
    message_1.setBody("message 1");

    var message_2 = try message_pool.create();
    defer message_pool.destroy(message_2);

    message_2.* = Message.new();
    message_2.headers.message_type = .publish;
    message_2.setTopicName("hello");
    message_2.setBody("message 2");

    var topic = try Topic.init(allocator, "hello", &message_pool);
    defer topic.deinit();

    const subscriber_1_id = uuid.v7.new();
    const subscriber_2_id = uuid.v7.new();

    try testing.expectEqual(0, topic.subscribers.count());

    var subscriber_1 = try topic.subscribe(subscriber_1_id);
    defer topic.unsubscribe(subscriber_1_id);

    var subscriber_2 = try topic.subscribe(subscriber_2_id);
    defer topic.unsubscribe(subscriber_2_id);

    try testing.expectEqual(2, topic.subscribers.count());
    topic.queue.enqueue(message_1);
    topic.queue.enqueue(message_2);

    try testing.expectEqual(0, subscriber_1.queue.count);
    try testing.expect(subscriber_1.queue.capacity > topic.queue.count);

    try testing.expectEqual(0, subscriber_2.queue.count);
    try testing.expect(subscriber_2.queue.capacity > topic.queue.count);

    try topic.tick();

    try testing.expectEqual(2, subscriber_1.queue.count);
    try testing.expectEqual(2, subscriber_2.queue.count);

    try testing.expectEqual(2, message_1.refs());
    try testing.expectEqual(2, message_2.refs());

    var subscriber_1_message_1 = subscriber_1.queue.dequeue().?;
    try testing.expectEqual(message_1, subscriber_1_message_1);
    subscriber_1_message_1.deref();

    var subscriber_1_message_2 = subscriber_1.queue.dequeue().?;
    try testing.expectEqual(message_2, subscriber_1_message_2);
    subscriber_1_message_2.deref();

    var subscriber_2_message_1 = subscriber_2.queue.dequeue().?;
    try testing.expectEqual(message_1, subscriber_2_message_1);
    subscriber_2_message_1.deref();

    var subscriber_2_message_2 = subscriber_2.queue.dequeue().?;
    try testing.expectEqual(message_2, subscriber_2_message_2);
    subscriber_2_message_2.deref();
}
