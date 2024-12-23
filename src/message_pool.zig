const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.MessagePool);

const constants = @import("constants.zig");
const Message = @import("./message.zig").Message;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;

/// MessagePool acts like a global allocator with a fixed number of pointers avaialble for use.
/// Each allocated pointer will be valid throughout the lifespan of the MessagePool but will be
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

    mutex: std.Thread.Mutex,

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
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *MessagePool) void {
        self.unassigned_queue.reset();
        self.assigned_map.deinit();
        self.messages.deinit();
    }

    pub fn create(self: *MessagePool) !*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

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
        self.mutex.lock();
        defer self.mutex.unlock();

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
