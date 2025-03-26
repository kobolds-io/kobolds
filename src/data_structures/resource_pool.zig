const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const MessageQueue = @import("./message_queue.zig").MessageQueue;

/// This is a work in progress. I want to replace the MessagePool with this generic data structure.
/// This data structure will focus on being more thread safe, generic and efficient.
pub fn ResourcePool(comptime T: type) type {
    _ = T;
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        capacity: u32,
        free_list: MessageQueue,

        pub fn init(allocator: std.mem.Allocator, capacity: u32) !void {
            assert(capacity > 0);

            // var free_list = Queue(T).new();
            _ = allocator;

            //     assert(capacity > 0);

            //     var unassigned_queue = MessageQueue.new(capacity);

            //     var items = try std.ArrayList(T).initCapacity(allocator, unassigned_queue.capacity);
            //     errdefer items.deinit();

            //     // fill the messages list with unintialized messages
            //     for (0..unassigned_queue.capacity) |_| {
            //         items.appendAssumeCapacity(T);
            //     }

            //     for (items.items) |*message| {
            //         try unassigned_queue.enqueue(message);
            //     }

            //     // ensure that the free queue is fully stocked with free messages
            //     assert(unassigned_queue.count == unassigned_queue.capacity);
            //     assert(items.items.len == unassigned_queue.count);

            //     return Self{
            //         .messages = items,
            //         .assigned_map = std.AutoHashMap(T, bool).init(allocator),
            //         .unassigned_queue = unassigned_queue,
            //         .capacity = capacity,
            //         .mutex = std.Thread.Mutex{},
            //     };
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }
    };
}

test "init/deinit" {
    //     const capacity: u32 = 10;
    //     const allocator = testing.allocator;

    //     var resource_pool = try ResourcePool(u8).init(allocator, capacity);
    //     defer resource_pool.deinit();
}
