const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Subscriber);

const uuid = @import("uuid");

const Message = @import("../protocol/message.zig").Message;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;

pub const Subscriber = struct {
    const Self = @This();

    id: uuid.Uuid,
    queue: *RingBuffer(*Message),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, id: uuid.Uuid) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, 1000);
        errdefer queue.deinit();

        return Self{
            .allocator = allocator,
            .id = id,

            .queue = queue,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();
        self.allocator.destroy(self.queue);
    }

    pub fn publish(self: *Self) !void {
        _ = self;
    }

    pub fn gather(self: *Self, queue: *RingBuffer(*Message)) !void {
        // TODO: the subscriber queue should be able to be concatenated onto another queue
        _ = self;
        _ = queue;
        // while(queue.available() > 0) {
        //
        //     if (self.queue.dequeue()) |message| {
        //
        // }
        // }
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var sub = try Subscriber.init(allocator, uuid.v7.new());
    defer sub.deinit();
}

test "notify subscribers" {}
