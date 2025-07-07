const std = @import("std");
const log = std.log.scoped(.Subscriber);
const assert = std.debug.assert;

const uuid = @import("uuid");
const utils = @import("../utils.zig");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const Topic = @import("./topic.zig").Topic;
const Message = @import("../protocol/message.zig").Message;

pub const Subscriber = struct {
    const Self = @This();

    conn_id: uuid.Uuid,
    queue: *RingBuffer(*Message),
    topic: *Topic,
    key: u128, // NOTE: a unique made of the topic_name and conn_id

    pub fn new(conn_id: uuid.Uuid, queue: *RingBuffer(*Message), topic: *Topic) Self {
        const key = utils.generateKey(topic.topic_name, conn_id);

        return Self{
            .conn_id = conn_id,
            .queue = queue,
            .topic = topic,
            .key = key,
        };
    }

    pub fn subscribe(self: *Self) !void {
        try self.topic.subscribe(self);
    }

    pub fn unsubscribe(self: *Self) void {
        self.topic.unsubscribe(self.key);
    }
};
