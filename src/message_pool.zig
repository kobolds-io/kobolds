const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Pool);

const constants = @import("./constants.zig");
const Message = @import("./message.zig").Message;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;

/// The Pool acts like a global allocator with a fixed number of pointers availble for use.
/// Each allocated pointer will be valid throughout the lifespan of the Pool but will be
/// reused by other messages.
pub const MessagePool = struct {
    /// Queue of message pointers that can be freely overriden. The message pointer
    /// should be removed from the queue once it's value has been overriden.
    unassigned_queue: MessageQueue,

    /// map of all assigned pointers
    /// TODO: This might not be the best datastructure for this but since the max queue size is so small (for now)
    /// i think that this might be okish. It might prove to be the bottleneck as more messages are put through the
    /// system.
    assigned_map: std.AutoHashMap(*Message, bool),

    /// backing array list that actually holds the value of each message
    messages: std.ArrayList(Message),

    pub fn init(allocator: std.mem.Allocator, message_pool_size: u32) !MessagePool {
        assert(message_pool_size <= constants.message_pool_max_size);
        assert(message_pool_size > 0);

        var unassigned_queue = MessageQueue.new(message_pool_size);
        var messages = try std.ArrayList(Message).initCapacity(allocator, unassigned_queue.max_size);
        errdefer messages.deinit();

        // fill the messages list with unintialized messages
        for (0..unassigned_queue.max_size) |_| {
            messages.appendAssumeCapacity(Message.new());
        }

        for (messages.items) |*message| {
            try unassigned_queue.enqueue(message);
        }

        // ensure that the free queue is fully stocked with free messages
        assert(unassigned_queue.count == unassigned_queue.max_size);
        assert(messages.items.len == unassigned_queue.count);

        return MessagePool{
            .messages = messages,
            .assigned_map = std.AutoHashMap(*Message, bool).init(allocator),
            .unassigned_queue = unassigned_queue,
        };
    }

    pub fn deinit(self: *MessagePool) void {
        self.unassigned_queue.reset();
        self.assigned_map.deinit();
        self.messages.deinit();
    }

    pub fn tick(self: *MessagePool) !void {
        var assigned_messages_iter = self.assigned_map.keyIterator();
        while (assigned_messages_iter.next()) |k| {
            if (k.*.ref_count == 0) {
                // TODO: this could be an unsafe destroy call where we skip all safety checks and just
                // absolutely nuke this message to get it ready as fast as possible
                self.destroy(k.*);
            }
        }

        // TODO: there should be some sort of tracking to ensure that messages aren't hogging resources
        // for too long. This could happen if there is a message that get's stuck somewhere
    }

    pub fn create(self: *MessagePool) !*Message {
        if (self.unassigned_queue.count == 0) return error.OutOfMemory;
        if (self.unassigned_queue.dequeue()) |ptr| {
            try self.assigned_map.put(ptr, true);

            // zero out the the pointer completely
            ptr.* = Message.new();

            assert(ptr.ref_count == 0);
            return ptr;
        } else unreachable;
    }

    pub fn destroy(self: *MessagePool, message_ptr: *Message) void {
        assert(message_ptr.ref_count == 0);

        // free the ptr from the assinged queue and give it back to the unassigned queue
        const res = self.assigned_map.remove(message_ptr);
        if (!res) {
            log.err("message_ptr did not exist in message pool {*}, {any}", .{ message_ptr, message_ptr.headers.message_type });
            unreachable;
        }

        message_ptr.next = null;
        self.unassigned_queue.enqueue(message_ptr) catch unreachable;
    }
};

test "create" {
    // Create a message pool
    const allocator = std.testing.allocator;

    var pool = try MessagePool.init(allocator, 100);
    defer pool.deinit();

    try testing.expectEqual(0, pool.assigned_map.count());

    const message = try pool.create();
    defer pool.destroy(message);

    try testing.expectEqual(1, pool.assigned_map.count());

    for (0..pool.unassigned_queue.count) |_| {
        _ = try pool.create();
    }

    // try and add one more and get the error
    try testing.expectError(error.OutOfMemory, pool.create());
}

test "destroy" {
    // Create a message pool
    const allocator = std.testing.allocator;

    var pool = try MessagePool.init(allocator, 100);
    defer pool.deinit();

    try testing.expectEqual(0, pool.assigned_map.count());

    const message = try pool.create();

    try testing.expectEqual(1, pool.assigned_map.count());
    pool.destroy(message);

    try testing.expectEqual(0, pool.assigned_map.count());
}

test "tick" {
    // Create a message pool
    const allocator = std.testing.allocator;

    var pool = try MessagePool.init(allocator, 100);
    defer pool.deinit();

    try testing.expectEqual(0, pool.assigned_map.count());

    for (0..50) |_| {
        var message = try pool.create();
        message.ref();
    }

    try testing.expectEqual(50, pool.assigned_map.count());

    // loop over each of the messages and deref a them
    try pool.tick();

    // ensure that nothing was destroyed
    try testing.expectEqual(50, pool.assigned_map.count());

    var assigned_messages_iter = pool.assigned_map.keyIterator();
    while (assigned_messages_iter.next()) |message| {
        message.*.deref();
    }

    try pool.tick();

    try testing.expectEqual(0, pool.assigned_map.count());
}
