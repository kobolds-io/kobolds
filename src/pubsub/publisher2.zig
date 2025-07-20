const std = @import("std");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;

pub const Publisher = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    buffer: RingBuffer(*Message),
    mutex: std.Thread.Mutex,
    memory_pool: *MemoryPool(Message),
    conn_id: u128,

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        conn_id: u128,
        capacity: usize,
    ) !Self {
        const buffer = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(buffer);

        buffer.* = try RingBuffer(*Message).init(allocator, capacity);
        errdefer buffer.deinit();

        return Self{
            .allocator = allocator,
            .buffer = buffer,
            .memory_pool = memory_pool,
            .mutex = std.Thread.Mutex{},
            .conn_id = conn_id,
        };
    }

    pub fn deinit(self: *Self) void {
        while (self.buffer.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.buffer.deinit();

        self.allocator.destroy();
    }
};
