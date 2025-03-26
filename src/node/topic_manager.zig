const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.Topic);

const uuid = @import("uuid");

const Topic = @import("./topic_2.zig").Topic;
const Message = @import("../protocol/message.zig").Message;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;
const EventEmitter = @import("../data_structures/event_emitter.zig").EventEmitter;
const UnmanagedQueue = @import("../data_structures/unmanaged_queue.zig").UnmanagedQueue;

pub const TopicManager = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    topics: std.StringHashMap(*Topic),
    topics_mutex: std.Thread.Mutex,
    message_pool: *MessagePool,

    pub fn init(allocator: std.mem.Allocator, message_pool: *MessagePool) Self {
        return Self{
            .allocator = allocator,
            .topics = std.StringHashMap(*Topic).init(allocator),
            .topics_mutex = std.Thread.Mutex{},
            .message_pool = message_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |topic_ptr| {
            const topic = topic_ptr.*;

            topic.deinit();
            self.allocator.destroy(topic);
        }

        self.topics.deinit();
    }

    pub fn tick(self: *Self) !void {
        if (self.topics.count() == 0) return;

        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        // loop over each topic and tick them
        var topics_iter = self.topics.valueIterator();
        while (topics_iter.next()) |entry| {
            const topic = entry.*;

            try topic.tick();
        }
    }

    pub fn enqueueTopicMessages(self: *Self, topic_name: []const u8, new_messages: *MessageQueue) !void {
        self.topics_mutex.lock();
        defer self.topics_mutex.unlock();

        if (self.topics.get(topic_name)) |topic| {
            topic.queue.concatenate(new_messages);
            new_messages.clear();
        } else {
            const topic = try self.addTopic(topic_name);

            topic.queue.concatenate(new_messages);
            new_messages.clear();
        }
    }

    fn addTopic(self: *Self, topic_name: []const u8) !*Topic {
        if (self.topics.contains(topic_name)) return error.TopicExists;

        const topic = try self.allocator.create(Topic);
        errdefer self.allocator.destroy(topic);

        topic.* = try Topic.init(self.allocator, topic_name, self.message_pool);
        errdefer topic.deinit();

        try self.topics.put(topic_name, topic);

        return topic;
    }
};

// const TopicEvent = enum {
//     publish,
//     subscribe,
//     unsubscribe,
//     close,
// };
//
// pub const Topic = struct {
//     const Self = @This();
//
//     allocator: std.mem.Allocator,
//     publish_queue: UnmanagedQueue(Message),
//     ee: EventEmitter(TopicEvent, *UnmanagedQueue(Message).NodeType),
//     topic_name: []const u8,
//     mutex: std.Thread.Mutex,
//
//     pub fn init(allocator: std.mem.Allocator, topic_name: []const u8) !Self {
//         const t_name = try allocator.alloc(u8, topic_name.len);
//         errdefer allocator.free(t_name);
//
//         @memcpy(t_name, topic_name);
//
//         return Self{
//             .topic_name = t_name,
//             .allocator = allocator,
//             .publish_queue = UnmanagedQueue(Message).new(),
//             .ee = EventEmitter(TopicEvent, *UnmanagedQueue(Message).NodeType).init(allocator),
//             .mutex = std.Thread.Mutex{},
//         };
//     }
//
//     pub fn deinit(self: *Self) void {
//         self.ee.deinit();
//         self.allocator.free(self.topic_name);
//     }
//
//     pub fn publishAll(self: *Self) void {
//         while (self.publish_queue.dequeue()) |node| {
//             self.ee.emit(.publish, node);
//         }
//     }
//
//     /// Be notified any time a message has been published to this topic.
//     pub fn subscribe(
//         self: *Self,
//         event: TopicEvent,
//         callback: EventEmitter(TopicEvent, *UnmanagedQueue(Message).NodeType).ListenerCallback,
//     ) !void {
//         try self.ee.addEventListener(event, callback);
//     }
//
//     /// Unsubcribe from notifications of this event
//     pub fn unsubscribe(
//         self: *Self,
//         event: TopicEvent,
//         callback: EventEmitter(TopicEvent, *UnmanagedQueue(Message).NodeType).ListenerCallback,
//     ) bool {
//         return self.ee.removeEventListener(event, callback);
//     }
// };

test "api design" {
    // 1. adding messages to be published
    //  topic.append(&unmanaged_queue)
    // 2. taking messages from the queue
    //  worker -> topic.
}

test "init/deinit" {
    // const allocator = testing.allocator;
    //
    // var topic = Topic.init(allocator, "/test");
    // defer topic.deinit();
    //
    // const sub_cb = struct {
    //     pub fn cb(event: TopicEvent, data: *Message) void {
    //         _ = event;
    //         _ = data;
    //     }
    // }.cb;
    //
    // try topic.subscribe(.publish, sub_cb);
    // defer _ = topic.unsubscribe(.publish, sub_cb);
    //
    // var message = Message.new();
    // message.headers.message_type = .publish;
    // message.setTopicName(topic.topic_name);
    //
    // topic.publish(&message);
}
