const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.ConnectionMessages);

const uuid = @import("uuid");

const RingBuffer = @import("stdx").RingBuffer;

const Message = @import("../protocol/message.zig").Message;

/// Used to map messages to their intended recipients
pub const ConnectionMessages = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    map: std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .map = std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // loop over all the values in the map and deinit the array lists
        var connection_map_iter = self.map.valueIterator();
        while (connection_map_iter.next()) |messages_queue_ptr| {
            const messages_queue = messages_queue_ptr.*;

            // TODO: we don't fully clean up here when we should
            while (messages_queue.dequeue()) |message| {
                message.deref();
            }

            messages_queue.deinit();
            self.allocator.destroy(messages_queue);
        }

        self.map.deinit();
    }

    pub fn append(self: *Self, conn_id: uuid.Uuid, message: *Message) !void {
        if (self.map.get(conn_id)) |queue| {
            try queue.enqueue(message);
        } else {
            // we need to create a new list for this uuid
            // create a new arraylist for this connection
            const queue = try self.allocator.create(RingBuffer(*Message));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(*Message).init(self.allocator, 1_000);
            errdefer queue.deinit();

            try queue.enqueue(message);

            try self.map.put(conn_id, queue);
        }
    }

    pub fn remove(self: *Self, conn_id: uuid.Uuid) bool {
        if (self.map.fetchRemove(conn_id)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            return true;
        } else {
            return false;
        }
    }
};

test "append a message" {
    const allocator = testing.allocator;

    var connection_messages = ConnectionMessages.init(allocator);
    defer connection_messages.deinit();

    var message_1 = Message.new();
    message_1.headers.message_type = .ping;
    message_1.headers.connection_id = 2;
    message_1.ref();

    const conn_id = uuid.v7.new();

    try testing.expect(connection_messages.map.get(conn_id) == null);

    try connection_messages.append(conn_id, &message_1);

    try testing.expect(connection_messages.map.get(conn_id) != null);

    const list = connection_messages.map.get(conn_id).?;
    try testing.expectEqual(1, list.count);

    var message_2 = Message.new();
    message_2.headers.message_type = .ping;
    message_2.headers.connection_id = 2;
    message_2.ref();

    try connection_messages.append(conn_id, &message_2);
    try testing.expectEqual(2, list.count);
}

test "init/deinit" {
    const allocator = testing.allocator;

    var grouper = ConnectionMessages.init(allocator);
    defer grouper.deinit();
}
