const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.MessagePool);

const constants = @import("../constants.zig");

const Message = @import("../protocol/message.zig").Message;
const MessageQueue = @import("./message_queue.zig").MessageQueue;
const RingBuffer = @import("stdx").RingBuffer;

/// The Pool acts like a global allocator with a fixed number of pointers availble for use.
/// Each allocated pointer will be valid throughout the lifespan of the Pool but will be
/// reused by other messages.
pub const MessagePool = struct {
    const Self = @This();

    assigned_map: std.AutoHashMap(*Message, bool),
    capacity: u32,
    free_list: RingBuffer(*Message),
    messages: std.ArrayList(Message),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, capacity: u32) !Self {
        assert(capacity > 0);

        var free_queue = try RingBuffer(*Message).init(allocator, capacity);
        errdefer free_queue.deinit();

        var messages = try std.ArrayList(Message).initCapacity(allocator, capacity);
        errdefer messages.deinit();

        // fill the messages list with unintialized messages
        for (0..capacity) |_| {
            messages.appendAssumeCapacity(Message.new());
        }

        for (messages.items) |*message| {
            free_queue.enqueue(message) catch @panic("unable to enqueue item");
        }

        // ensure that the free queue is fully stocked with free messages
        assert(messages.items.len == free_queue.count);

        return Self{
            .assigned_map = std.AutoHashMap(*Message, bool).init(allocator),
            .capacity = capacity,
            .free_list = free_queue,
            .messages = messages,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *MessagePool) void {
        self.free_list.deinit();
        self.assigned_map.deinit();
        self.messages.deinit();
    }

    /// Count of currently assigned messages
    pub fn count(self: *MessagePool) u32 {
        // this might be slow??
        return self.assigned_map.count();
    }

    /// Count of messages that are available to be taken
    pub fn available(self: *MessagePool) u32 {
        return self.free_list.count;
    }

    pub fn create(self: *MessagePool) !*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.unsafeCreate();
    }

    /// Doesn't lock the message pool when creating pointer
    pub fn unsafeCreate(self: *MessagePool) !*Message {
        if (self.available() == 0) return error.OutOfMemory;

        if (self.free_list.dequeue()) |message_ptr| {
            try self.assigned_map.put(message_ptr, true);

            return message_ptr;
        } else unreachable;
    }

    pub fn createN(self: *MessagePool, allocator: std.mem.Allocator, n: u32) ![]*Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.unsafeCreateN(allocator, n);
    }

    /// Doesn't lock the message pool when creating pointers
    pub fn unsafeCreateN(self: *MessagePool, allocator: std.mem.Allocator, n: u32) ![]*Message {
        if (self.available() < n) return error.OutOfMemory;

        var list = try std.ArrayList(*Message).initCapacity(allocator, n);
        errdefer list.deinit();

        for (0..n) |_| {
            if (self.free_list.dequeue()) |message_ptr| {
                try list.append(message_ptr);
                try self.assigned_map.put(message_ptr, true);
            } else break;
        }

        return list.toOwnedSlice();
    }

    pub fn destroy(self: *MessagePool, message_ptr: *Message) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.unsafeDestroy(message_ptr);
    }

    /// Doesn't lock the message pool when destroying pointers
    pub fn unsafeDestroy(self: *MessagePool, message_ptr: *Message) void {
        assert(message_ptr.refs() == 0);

        // free the ptr from the assinged queue and give it back to the unassigned queue
        const res = self.assigned_map.remove(message_ptr);
        if (!res) {
            log.err("message_ptr did not exist in message pool {*}, {any}", .{
                message_ptr,
                message_ptr.headers.message_type,
            });
            unreachable;
        }

        self.free_list.enqueue(message_ptr) catch |err| {
            log.err("error enqueueuing message_ptr {any}", .{err});
            @panic("unable to enqueue destroyed message");
        };
    }
};

test "create a pointer" {
    // Create a message pool
    const allocator = std.testing.allocator;

    var pool = try MessagePool.init(allocator, 100);
    defer pool.deinit();

    try testing.expectEqual(0, pool.assigned_map.count());

    const message = try pool.create();
    defer pool.destroy(message);

    try testing.expectEqual(1, pool.assigned_map.count());

    for (0..pool.free_list.count) |_| {
        _ = try pool.create();
    }

    // try and add one more and get the error
    try testing.expectError(error.OutOfMemory, pool.create());
}

test "create n pointers" {
    const allocator = std.testing.allocator;

    // Initialize the pool
    var pool = try MessagePool.init(allocator, 10);
    defer pool.deinit();

    try testing.expectEqual(0, pool.assigned_map.count());

    // get a bunch of messages
    const ptrs = try pool.createN(allocator, 10);
    defer allocator.free(ptrs);

    try testing.expectEqual(10, ptrs.len);

    try testing.expectError(error.OutOfMemory, pool.createN(allocator, 1));
}

test "destroy n pointers" {
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
