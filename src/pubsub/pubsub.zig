const std = @import("std");
const testing = std.testing;
const Message = @import("../protocol/message.zig").Message;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;

const Topic = struct {
    const Self = @This();
    allocator: std.mem.Allocator,

    /// list of all messages
    message_map: std.AutoHashMap(*Message, std.mem.Allocator),
    unpublished_messages: RingBuffer(*Message),
    subscribers: std.AutoHashMap(*Subscriber, bool),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .message_map = std.AutoHashMap(*Message, std.mem.Allocator).init(allocator),
            .unpublished_messages = RingBuffer(*Message).init(allocator, 100) catch unreachable,
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

        // FIX: there has to be more complicated cleanup for this
        self.message_map.deinit();
        self.unpublished_messages.deinit();
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

    pub fn tick(self: *Self) !void {
        try self.publish();
        try self.prune();
    }

    fn publish(self: *Self) !void {
        // perhaps we have a buffer that we keep on disk. For example, if we have 1_000_000 messages,
        // we might not want to keep all of those in memory because it is unrealistic that we would be able
        // to process them all at the same time. Perhaps past a certain limit, say 50_000 messages, we write new
        // messages to disk. On every tick we read work through what we have in memory and then refill our
        // in memory buffer with messages we read from disk. This might need to be a totally special allocator
        // that belongs to the topic and not the publisher. So slightly different but same pattern overall.

        while (self.unpublished_messages.dequeue()) |message| {
            var subs = self.subscribers.keyIterator();
            while (subs.next()) |entry| {
                const sub = entry.*;
                message.ref();

                // we would want to check how many messages are in the queue and if we are getting full,
                // then we should dequeue the oldest item and dereference it so it can be cleaned up later.
                // This would mean that some subscribers might be so slow that they cannot process all of the
                // messages and will only receive "some" messages.
                if (sub.messages.available() == 0) {
                    const dropped_message = sub.messages.dequeue().?;
                    dropped_message.deref();
                    // log an error here
                }
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

const Publisher = struct {
    const Self = @This();
    allocator: std.mem.Allocator,
    connection: u32,
    topic: *Topic,

    pub fn init(allocator: std.mem.Allocator, connection: u32, topic: *Topic) Self {
        return Self{
            .allocator = allocator,
            .connection = connection,
            .topic = topic,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    pub fn publish(self: *Self, allocator: std.mem.Allocator, message: *Message) !void {
        self.topic.mutex.lock();
        defer self.topic.mutex.unlock();

        try self.topic.message_map.put(message, allocator);
        try self.topic.unpublished_messages.enqueue(message);
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

    // on the publisher worker thread
    try publisher.publish(allocator, message);

    try testing.expectEqual(0, subscriber.messages.count);

    // tick the topic to publish the message to all subscribers
    try topic.tick();

    try testing.expectEqual(1, subscriber.messages.count);

    // NOTE: this would be like taking the messages out of the subsriber and putting them in the connection's outbox.
    // A more direct alternative that doesn't really require any futher operations is to simply put the messages
    // directly into the connection's outbox, therefore removing the need for an additional tick.
    try subscriber.gather();

    try testing.expectEqual(0, subscriber.messages.count);

    // tick the topic twice
    try topic.tick();

    try testing.expectEqual(0, topic.message_map.count());
}

// test "self destruction" {
//     const allocator = testing.allocator;
//
//     const envelope = try allocator.create(Envelope);
//     envelope.* = Envelope{
//         .allocator = allocator,
//         .data = 10,
//         .ref_count = std.atomic.Value(u32).init(0),
//         .mutex = std.Thread.Mutex{},
//     };
//
//     envelope.ref();
//
//     try testing.expectEqual(1, envelope.refs());
//
//     envelope.deref();
//
//     try testing.expectEqual(0, envelope.refs());
//     if (envelope.refs() == 0) {
//         const env_allocator = envelope.allocator;
//         env_allocator.destroy(envelope);
//     }
// }
