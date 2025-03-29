const std = @import("std");
const testing = std.testing;
const Message = @import("../protocol/message.zig").Message;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;

const Topic = struct {
    const Self = @This();
    allocator: std.mem.Allocator,

    /// list of all messages
    subscribers: std.AutoHashMap(*Subscriber, bool),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .subscribers = std.AutoHashMap(*Subscriber, bool).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        var subs = self.subscribers.keyIterator();
        while (subs.next()) |entry| {
            const sub = entry.*;
            sub.deinit();

            self.allocator.destroy(sub);
        }

        self.subscribers.deinit();
    }

    pub fn subscribe(self: *Self) !*Subscriber {
        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = Subscriber.init(self.allocator, 1, self);
        errdefer subscriber.deinit();

        try self.subscribers.put(subscriber, true);
        return subscriber;
    }

    pub fn unsubscribe(self: *Self, subscriber_ptr: *Subscriber) void {
        if (self.subscribers.getKey(subscriber_ptr)) |sub| {
            sub.deinit();
            _ = self.subscribers.remove(subscriber_ptr);
            self.allocator.destroy(subscriber_ptr);
        }
    }

    fn prune(self: *Self) !void {
        // clean out old dead messages from the topic
        var messages_iter = self.message_map.iterator();
        while (messages_iter.next()) |entry| {
            const message = entry.key_ptr.*;
            const message_allocator = entry.value_ptr.*;

            if (message.refs() > 0) continue;

            // there would need to be a lock on this destroy. So a threadsafe memory pool is still required
            // but this guarantees that I only ever need a single allocation for this message which is beneficial,
            // perhaps more beneficial than anything else.
            message_allocator.destroy(message);
            self.message_map.removeByPtr(entry.key_ptr);
        }
    }
};

const Publisher = struct {
    const Self = @This();
    allocator: std.mem.Allocator,
    connection: u32,
    topic: *Topic,
    message_map: std.AutoHashMap(*Message, std.mem.Allocator),
    unpublished_messages: RingBuffer(*Message),

    pub fn init(allocator: std.mem.Allocator, connection: u32, topic: *Topic) Self {
        return Self{
            .allocator = allocator,
            .message_map = std.AutoHashMap(*Message, std.mem.Allocator).init(allocator),
            .unpublished_messages = RingBuffer(*Message).init(allocator, 100) catch unreachable,
            .connection = connection,
            .topic = topic,
        };
    }

    pub fn deinit(self: *Self) void {
        self.message_map.deinit();
        self.unpublished_messages.deinit();
    }

    pub fn enqueueMessage(self: *Self, allocator: std.mem.Allocator, message: *Message) !void {
        try self.message_map.put(message, allocator);
        try self.unpublished_messages.enqueue(message);
    }

    pub fn publish(self: *Self) !void {
        while (self.unpublished_messages.dequeue()) |message| {
            self.topic.mutex.lock();
            defer self.topic.mutex.unlock();

            const subscribers = self.topic.subscribers;

            var sub_iter = subscribers.iterator();
            while (sub_iter.next()) |entry| {
                // lock the subscriber
                const sub = entry.key_ptr.*;

                sub.mutex.lock();
                defer sub.mutex.unlock();

                message.ref();
                try sub.messages.enqueue(message);
            }
        }
    }

    fn prune(self: *Self) !void {
        // clean out old dead messages from the topic
        var messages_iter = self.message_map.iterator();
        while (messages_iter.next()) |entry| {
            const message = entry.key_ptr.*;
            const message_allocator = entry.value_ptr.*;

            if (message.refs() > 0) continue;

            // there would need to be a lock on this destroy. So a threadsafe memory pool is still required
            // but this guarantees that I only ever need a single allocation for this message which is beneficial,
            // perhaps more beneficial than anything else.
            message_allocator.destroy(message);
            self.message_map.removeByPtr(entry.key_ptr);
        }
    }
};

const Subscriber = struct {
    const Self = @This();
    messages: RingBuffer(*Message),
    connection: u32,
    allocator: std.mem.Allocator,
    topic: *Topic,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, connection: u32, topic: *Topic) Self {
        return Self{
            .allocator = allocator,
            .connection = connection,
            .messages = RingBuffer(*Message).init(allocator, 100) catch unreachable,
            .topic = topic,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.messages.deinit();
    }

    pub fn gather(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // on the subscriber worker thread which is a subscriber.tick();
        if (self.messages.count > 0) {
            while (self.messages.dequeue()) |message_ref| {
                // this would be copying the messages queue to the
                message_ref.deref();
            }
        }
    }
};

// NOTE: In this version, the topic is just a holder of information. It's job is simply to hold the subscribers/publishers.
// the publisher's job is to directly write messages onto the subscriber's queue.
// The subscriber then processes the message and the publisher eventually prunes the message
//
//
// NOTE: A possibly better situation would be for the Topic to still hold onto ALL of the messages<->allocators and it
// tick along in the background without necessarilly requiring the publisher to deal with the cleanup of the message.
test "publisher to subscriber" {
    const allocator = testing.allocator;

    const message = try allocator.create(Message);
    // defer allocator.destroy(message);

    message.* = Message.new();
    message.headers.message_type = .ping;
    message.setTransactionId(1);
    try testing.expectEqual(null, message.validate());

    var topic = Topic.init(allocator);
    defer topic.deinit();

    const subscriber = try topic.subscribe();
    defer topic.unsubscribe(subscriber);

    var publisher = Publisher.init(allocator, 1, &topic);
    defer publisher.deinit();

    try publisher.enqueueMessage(allocator, message);

    try testing.expectEqual(0, subscriber.messages.count);

    // on the publisher worker thread
    try publisher.publish();

    try testing.expectEqual(1, subscriber.messages.count);

    try subscriber.gather();

    try testing.expectEqual(0, subscriber.messages.count);

    try publisher.prune();
}
