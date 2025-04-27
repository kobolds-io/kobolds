const std = @import("std");

const Topic = @import("./topic.zig").Topic;

pub const TopicManager = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    topics: std.StringHashMap(*Topic),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .topics = std.StringHashMap(*Topic).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var topics_iterator = self.topics.valueIterator();
        while (topics_iterator.next()) |entry| {
            const topic = entry.*;

            topic.deinit();
        }
    }

    pub fn get(self: *Self, topic_name: []const u8) ?*Topic {
        return self.topics.get(topic_name);
    }

    pub fn create(self: *Self, topic_name: []const u8) !*Topic {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.topics.contains(topic_name)) return error.TopicExists;

        const topic = try self.allocator.create(Topic);
        errdefer self.allocator.destroy(topic);

        topic.* = Topic.init(self.allocator, topic_name);
        errdefer topic.deinit();

        try self.topics.put(topic_name, topic);

        return topic;
    }
};
