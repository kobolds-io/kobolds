const std = @import("std");
const testing = std.testing;

const RingBuffer = @import("stdx").RingBuffer;

const constants = @import("../constants.zig");
const Envelope = @import("../node/envelope.zig").Envelope;

pub const Publisher = struct {
    queue: RingBuffer(Envelope),
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
};

test "init/deinit" {
    const allocator = testing.allocator;

    var topic = try Topic.init(allocator, "/test/topic", .{ .queue_capacity = 10 });
    defer topic.deinit(allocator);
}
