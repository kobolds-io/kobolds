const std = @import("std");
const log = std.log.scoped(.Topic);
const testing = std.testing;
const assert = std.debug.assert();

const RingBuffer = @import("stdx").RingBuffer;
const EventEmitter = @import("stdx").EventEmitter;

const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const Message = @import("../protocol/message.zig").Message;

const Publisher = @import("./publisher.zig").Publisher;
const Subscriber = @import("./subscriber.zig").Subscriber;

// const SubscriptionCallback = *const fn (event: TopicEvent, message: *Message) void;

pub const Topic = struct {
    const Self = @This();
    pub const TopicEvent = enum { publish };

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    publishers: std.AutoHashMap(u128, *Publisher),
    subscribers: std.AutoHashMap(u128, *Subscriber),
    topic_name: []const u8,
    ee: EventEmitter(TopicEvent, *Message),

    pub fn init(allocator: std.mem.Allocator, topic_name: []const u8) Self {
        return Self{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .publishers = std.AutoHashMap(u128, *Publisher).init(allocator),
            .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .topic_name = topic_name,
            .ee = EventEmitter(TopicEvent, *Message).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.subscribers.deinit();
        self.publishers.deinit();
        self.ee.deinit();
    }

    pub fn publish(self: *Self, message: *Message) !void {
        defer message.deref();

        self.mutex.lock();
        defer self.mutex.unlock();

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;

            subscriber.mutex.lock();
            defer subscriber.mutex.unlock();

            try subscriber.queue.enqueue(message);
        }

        self.ee.emit(.publish, message);

        // if (self.subscribers.count() == 0) {
        //     // TODO: we should write the message to disk
        //     // log.debug("no subscribers for topic {s}", .{self.topic_name});
        // }
    }

    pub fn publishMany(self: *Self, messages: []*Message) !void {
        if (messages.len == 0) return;
        if (self.subscribers.count() == 0) {
            // TODO: we should write the message to disk
            // log.debug("no subscribers for topic {s}", .{self.topic_name});
            for (messages) |message| {
                message.deref();
            }
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;

            subscriber.mutex.lock();
            defer subscriber.mutex.unlock();

            const n = subscriber.queue.enqueueMany(messages);

            // FIX: If not all the items will fit in the queue we should write these to disk
            // or something so that they can be published "later". The behavior should be similar
            // to how nginx buffers large client_body requests.
            if (n != messages.len) {
                @panic("subscriber could not handle message");
            }

            // quickly reference each message
            for (messages[0..n]) |message| {
                message.ref();
            }
        }
    }

    pub fn subscribe(self: *Self, subscriber: *Subscriber) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.subscribers.put(subscriber.key, subscriber);
        try self.ee.addEventListener(.publish, subscriber, subscriber.callback);
    }

    pub fn unsubscribe(self: *Self, key: u128) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.subscribers.get(key)) |subscriber| {
            _ = self.ee.removeEventListener(.publish, subscriber.callback);
            _ = self.subscribers.remove(key);
        }
    }
};

// test "topic init/deinit" {
//     const allocator = testing.allocator;
//
//     const topic_name = "/test/topic";
//     var topic = Topic.init(allocator, topic_name);
//     defer topic.deinit();
// }
//
// test "publish/subscribe" {
//     const allocator = testing.allocator;
//
//     const topic_name = "/test/topic";
//
//     // create a message to be published
//     var message_pool = try MessagePool.init(allocator, 100);
//     defer message_pool.deinit();
//
//     // NOTE: the message should be destroyed by the subscriber. Otherwise the message will leak
//     const message = try Message.create(&message_pool);
//     defer message.deref();
//     message.headers.message_type = .publish;
//     message.setTopicName(topic_name);
//
//     var topic = Topic.init(allocator, topic_name);
//     defer topic.deinit();
//
//     var subscriber = try Subscriber.init(allocator, 1, &topic);
//     defer subscriber.deinit();
//
//     try subscriber.subscribe();
//     defer subscriber.unsubscribe();
//
//     var publisher = try Publisher.init(allocator, 1, &topic);
//     defer publisher.deinit();
//
//     try testing.expectEqual(0, subscriber.queue.count);
//
//     try publisher.publish(message);
//
//     try testing.expectEqual(1, subscriber.queue.count);
//
//     // Pretend that we are "ticking" the subscriber
//     while (subscriber.queue.dequeue()) |subscriber_message| {
//         try testing.expectEqual(1, subscriber_message.refs());
//
//         subscriber_message.deref();
//     }
//
//     try testing.expectEqual(0, subscriber.queue.count);
// }
//
// test "publishMany" {
//     const allocator = testing.allocator;
//
//     const topic_name = "/test/topic";
//
//     // create a message to be published
//     var message_pool = try MessagePool.init(allocator, 100);
//     defer message_pool.deinit();
//
//     const messages_buf = try allocator.alloc(Message, 100);
//     defer allocator.free(messages_buf);
//
//     // NOTE: the message should be destroyed by the subscriber. Otherwise the message will leak
//     const messages = try message_pool.createN(allocator, @intCast(messages_buf.len));
//     defer allocator.free(messages);
//
//     for (messages, 0..messages_buf.len) |message, i| {
//         message.* = Message.new();
//         message.message_pool = &message_pool;
//         message.headers.message_type = .publish;
//         message.setTopicName(topic_name);
//
//         messages[i] = message;
//     }
//
//     var topic = Topic.init(allocator, topic_name);
//     defer topic.deinit();
//
//     var subscriber = try Subscriber.init(allocator, 1, &topic);
//     defer subscriber.deinit();
//
//     try subscriber.subscribe();
//     defer subscriber.unsubscribe();
//
//     var publisher = try Publisher.init(allocator, 1, &topic);
//     defer publisher.deinit();
//
//     try testing.expectEqual(0, subscriber.queue.count);
//
//     try publisher.publishMany(messages);
//
//     try testing.expectEqual(messages.len, subscriber.queue.count);
//
//     // Pretend that we are "ticking" the subscriber
//     while (subscriber.queue.dequeue()) |message| {
//         try testing.expectEqual(1, message.refs());
//
//         message.deref();
//     }
//
//     try testing.expectEqual(0, subscriber.queue.count);
// }
