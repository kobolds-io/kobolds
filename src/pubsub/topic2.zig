const std = @import("std");
const testing = std.testing;
const KID = @import("kid").KID;

const RingBuffer = @import("stdx").RingBuffer;

const constants = @import("../constants.zig");
const Envelope = @import("../node/envelope.zig").Envelope;

const Message = @import("../protocol/message2.zig").Message;

pub const PublisherOptions = struct {
    queue_capacity: usize = constants.publisher_max_queue_capacity,
};

pub const Publisher = struct {
    const Self = @This();

    queue: RingBuffer(Envelope),
    publisher_key: u64,

    pub fn init(allocator: std.mem.Allocator, publisher_key: u64, options: PublisherOptions) !Self {
        return Self{
            .queue = try RingBuffer(Envelope).init(allocator, options.queue_capacity),
            .publisher_key = publisher_key,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        _ = allocator;
        self.queue.deinit();
    }
};

pub const Subscriber = struct {
    queue: RingBuffer(Envelope),
};

pub const TopicOptions = struct {
    queue_capacity: usize = constants.topic_max_queue_capacity,
};

pub const Topic = struct {
    const Self = @This();

    topic_name: []const u8,
    publishers: std.AutoHashMapUnmanaged(u64, *Publisher),
    subscribers: std.AutoHashMapUnmanaged(u64, *Subscriber),
    queue: RingBuffer(Envelope),

    pub fn init(allocator: std.mem.Allocator, topic_name: []const u8, options: TopicOptions) !Self {
        return Self{
            .topic_name = try allocator.dupe(u8, topic_name),
            .publishers = .empty,
            .subscribers = .empty,
            .queue = try RingBuffer(Envelope).init(allocator, options.queue_capacity),
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.queue.deinit();
        self.publishers.deinit(allocator);
        self.subscribers.deinit(allocator);

        allocator.free(self.topic_name);
    }

    pub fn gatherEnvelopes(self: *Self) !void {
        // loop over all the publishers and grab their envelopes
        var publishers_iter = self.publishers.valueIterator();
        while (publishers_iter.next()) |entry| {
            const publisher = entry.*;

            self.queue.concatenateAvailable(&publisher.queue);

            // we cannot hold any more
            if (self.queue.available() == 0) break;
        }
    }

    pub fn distributeEnvelopes(self: *Self) !void {
        _ = self;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var topic = try Topic.init(allocator, "/test/topic", .{ .queue_capacity = 10 });
    defer topic.deinit(allocator);
}

test "gather envelopes from publishers" {
    //     const allocator = testing.allocator;

    //     var kid = KID.init(1, .{});

    //     var topic = try Topic.init(allocator, "/test/topic", .{ .queue_capacity = 10 });
    //     defer topic.deinit(allocator);

    //     for (0..topic.queue.capacity) |_| {
    //         var message_1 = Message.new(.publish);
    //         const envelope_1 = Envelope{
    //             .message = &message_1,
    //             .conn_id = 0,
    //             .session_id = 0,
    //             .message_id = kid.generate(),
    //         };
    //     }

    //     var message_1 = Message.new(.publish);
    //     const envelope_1 = Envelope{
    //         .message = &message_1,
    //         .conn_id = 0,
    //         .session_id = 0,
    //         .message_id = kid.generate(),
    //     };

    //     var message_2 = Message.new(.publish);
    //     const envelope_2 = Envelope{
    //         .message = &message_2,
    //         .conn_id = 0,
    //         .session_id = 0,
    //         .message_id = 2,
    //     };

    //     var message_3 = Message.new(.publish);
    //     const envelope_3 = Envelope{
    //         .message = &message_3,
    //         .conn_id = 0,
    //         .session_id = 0,
    //         .message_id = 3,
    //     };

    //     // create a publisher
    //     var publisher_1 = try Publisher.init(allocator, .{ .queue_capacity = 10 });
    //     defer publisher_1.deinit(allocator);

    //     var publisher_2 = try Publisher.init(allocator, .{ .queue_capacity = 10 });
    //     defer publisher_2.deinit(allocator);

    //     // add the messages to the publishers
    //     try publisher_1.queue.enqueue(envelope_1);
    //     try publisher_2.queue.enqueue(envelope_2);
    //     try publisher_2.queue.enqueue(envelope_3);

    //     // add all the publishers to this topic
    //     try topic.publishers.put(allocator, 1, &publisher_1);
    //     try topic.publishers.put(allocator, 2, &publisher_2);

    //     try testing.expectEqual(0, topic.queue.count);

    //     try topic.gatherEnvelopes();

    //     try testing.expectEqual(3, topic.queue.count);
}
