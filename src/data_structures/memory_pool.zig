const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.MemoryPool);

const UnmanagedQueue = @import("./unmanaged_queue.zig").UnmanagedQueue;
const UnmanagedQueueNode = @import("./unmanaged_queue.zig").Node;

pub fn MemoryPool(comptime T: type) type {
    return struct {
        const Self = @This();
        const NodeType = *UnmanagedQueueNode(T);

        allocator: std.mem.Allocator,
        assigned_map: std.AutoHashMap(*UnmanagedQueueNode(T), bool),
        backing_buffer: std.ArrayList(*UnmanagedQueueNode(T)),
        capacity: u32,
        free_list: UnmanagedQueue(T),
        mutex: std.Thread.Mutex,

        pub fn init(allocator: std.mem.Allocator, initial_capacity: u32) !Self {
            assert(initial_capacity > 0);

            var backing_buffer = try std.ArrayList(*UnmanagedQueueNode(T)).initCapacity(allocator, initial_capacity);
            errdefer backing_buffer.deinit();

            var free_list = UnmanagedQueue(T).new();

            for (0..initial_capacity) |_| {
                // create a new node for each item
                const n = try allocator.create(UnmanagedQueueNode(T));
                errdefer allocator.destroy(n);

                backing_buffer.appendAssumeCapacity(n);
                free_list.enqueue(n);
            }

            assert(free_list.count == initial_capacity);
            assert(backing_buffer.capacity == initial_capacity);

            return Self{
                .allocator = allocator,
                .assigned_map = std.AutoHashMap(*UnmanagedQueueNode(T), bool).init(allocator),
                .backing_buffer = backing_buffer,
                .capacity = initial_capacity,
                .free_list = free_list,
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            // drop all refs to nodes
            self.free_list.reset();

            // loop through all the pointers in the backing buffer and destroy them
            for (self.backing_buffer.items) |node| {
                self.allocator.destroy(node);
            }

            // deinit
            self.backing_buffer.deinit();
            self.assigned_map.deinit();
        }

        pub fn available(self: Self) u32 {
            return self.free_list.count;
        }

        /// Return a pointer to a node that will live until it is destroyed
        pub fn create(self: *Self) !*UnmanagedQueueNode(T) {
            if (self.available() == 0) return error.OutOfMemory;

            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.free_list.dequeue()) |node| {
                try self.assigned_map.put(node, true);

                return node;
            } else unreachable;
        }

        /// Create an arbitrary number of pointers
        pub fn createN(self: *Self, allocator: std.mem.Allocator, n: u32) ![]*UnmanagedQueueNode(T) {
            if (self.free_list.count == 0) return error.OutOfMemory;
            if (self.free_list.count -| n + 1 == 0) return error.OutOfMemory;

            var list = try std.ArrayList(*UnmanagedQueueNode(T)).initCapacity(allocator, n);
            errdefer list.deinit();

            self.mutex.lock();
            defer self.mutex.unlock();

            for (0..n) |_| {
                if (self.free_list.dequeue()) |node_ptr| {
                    try list.append(node_ptr);
                    try self.assigned_map.put(node_ptr, true);
                } else break;
            }

            return list.toOwnedSlice();
        }

        /// Destroy a pointer that can then be reclaimed by the memory pool.
        pub fn destroy(self: *Self, node: *UnmanagedQueueNode(T)) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            // free the ptr from the assinged_queue and give it back to the free_list
            const res = self.assigned_map.remove(node);
            if (!res) {
                // log.err("ptr did not exist in message pool {*} memory pool count {}", .{ node, self.available() });
                // unreachable;
                return;
            }

            self.free_list.enqueue(node);
        }
    };
}

test "init/deinit" {
    const allocator = testing.allocator;

    var memory_pool = try MemoryPool(u8).init(allocator, 100);
    defer memory_pool.deinit();

    try testing.expectEqual(100, memory_pool.capacity);
}

const TestStruct = struct {
    val: u8,
};

test "create/destroy" {
    const allocator = testing.allocator;

    var memory_pool = try MemoryPool(TestStruct).init(allocator, 100);
    defer memory_pool.deinit();

    const node = try memory_pool.create();

    const test_struct = TestStruct{ .val = 123 };
    node.data = test_struct;

    try testing.expectEqual(1, memory_pool.assigned_map.count());
    try testing.expectEqual(memory_pool.capacity - 1, memory_pool.free_list.count);

    memory_pool.destroy(node);

    var nodes = std.ArrayList(MemoryPool(TestStruct).NodeType).init(allocator);
    defer nodes.deinit();

    for (0..memory_pool.capacity) |i| {
        const n = try memory_pool.create();
        const ts = TestStruct{ .val = @intCast(i) };
        n.data = ts;
    }

    try testing.expectEqual(memory_pool.capacity, memory_pool.assigned_map.count());
    try testing.expectEqual(0, memory_pool.free_list.count);

    for (nodes.items) |n| {
        memory_pool.destroy(n);
    }
}

test "create an arbitrary number of nodes" {
    const allocator = std.testing.allocator;

    // Initialize the pool
    var memory_pool = try MemoryPool(u8).init(allocator, 10);
    defer memory_pool.deinit();

    try testing.expectEqual(10, memory_pool.available());

    // get a bunch of messages
    const ptrs = try memory_pool.createN(allocator, 10);
    defer allocator.free(ptrs);

    try testing.expectEqual(10, ptrs.len);

    try testing.expectEqual(0, memory_pool.available());

    try testing.expectError(error.OutOfMemory, memory_pool.createN(allocator, 1));
}
