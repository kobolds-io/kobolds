const std = @import("std");
const uuid = @import("uuid");
const utils = @import("../utils.zig");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const Topic = @import("./topic.zig").Topic;
const Message = @import("../protocol/message.zig").Message;

pub const Publisher = struct {
    const Self = @This();

    conn_id: uuid.Uuid,
    topic: *Topic,
    key: u128,
    published_count: u128,

    pub fn new(conn_id: uuid.Uuid, topic: *Topic) Self {
        const key = utils.generateKey(topic.topic_name, conn_id);

        return Self{
            .conn_id = conn_id,
            .topic = topic,
            .key = key,
            .published_count = 0,
        };
    }

    pub fn publish(self: *Self, message: *Message) !void {
        try self.topic.enqueue(message);
        self.published_count += 1;
    }
};
