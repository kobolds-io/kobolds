const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.ConnectionMessages);

const uuid = @import("uuid");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message2.zig").Message;

/// Used to map messages to their intended recipients
pub const ConnectionMessages = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    map: std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)),
    memory_pool: *MemoryPool(Message),

    pub fn init(allocator: std.mem.Allocator, memory_pool: *MemoryPool(Message)) Self {
        return Self{
            .allocator = allocator,
            .map = std.AutoHashMap(uuid.Uuid, *RingBuffer(*Message)).init(allocator),
            .memory_pool = memory_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        // loop over all the values in the map and deinit the array lists
        var connection_map_iter = self.map.valueIterator();
        while (connection_map_iter.next()) |messages_queue_ptr| {
            const messages_queue = messages_queue_ptr.*;

            while (messages_queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            messages_queue.deinit();
            self.allocator.destroy(messages_queue);
        }

        self.map.deinit();
    }

    pub fn add(self: *Self, conn_id: uuid.Uuid) !void {
        if (self.map.get(conn_id)) |_| {
            return error.AlreadyExists;
        } else {
            const queue = try self.allocator.create(RingBuffer(*Message));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(*Message).init(self.allocator, constants.connection_outbox_capacity);
            errdefer queue.deinit();

            try self.map.put(conn_id, queue);
        }
    }

    pub fn append(self: *Self, conn_id: uuid.Uuid, message: *Message) !void {
        if (self.map.get(conn_id)) |queue| {
            try queue.enqueue(message);
        } else {
            // we need to create a new list for this uuid
            // create a new arraylist for this connection
            const queue = try self.allocator.create(RingBuffer(*Message));
            errdefer self.allocator.destroy(queue);

            queue.* = try RingBuffer(*Message).init(self.allocator, constants.connection_outbox_capacity);
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

    var memory_pool = try MemoryPool(Message).init(allocator, 10);
    defer memory_pool.deinit();

    var connection_messages = ConnectionMessages.init(allocator, &memory_pool);
    defer connection_messages.deinit();

    const message_1 = try memory_pool.create();
    message_1.* = Message.new();
    message_1.headers.message_type = .ping;
    message_1.headers.connection_id = 2;
    message_1.ref();

    const conn_id = uuid.v7.new();

    try testing.expect(connection_messages.map.get(conn_id) == null);

    try connection_messages.append(conn_id, message_1);

    try testing.expect(connection_messages.map.get(conn_id) != null);

    const list = connection_messages.map.get(conn_id).?;
    try testing.expectEqual(1, list.count);

    const message_2 = try memory_pool.create();
    message_2.* = Message.new();
    message_2.headers.message_type = .ping;
    message_2.headers.connection_id = 2;
    message_2.ref();

    try connection_messages.append(conn_id, message_2);
    try testing.expectEqual(2, list.count);
}

test "init/deinit" {
    const allocator = testing.allocator;

    var memory_pool = try MemoryPool(Message).init(allocator, 10);
    defer memory_pool.deinit();

    var connection_messages = ConnectionMessages.init(allocator, &memory_pool);
    defer connection_messages.deinit();
}
