const std = @import("std");

pub fn RoundRobin(comptime T: type) type {
    return struct {
        const Self = @This();

        current_index: usize,
        list: std.ArrayList(T),

        pub fn init() Self {
            return Self{
                .current_index = 0,
                .list = .empty,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.list.deinit(allocator);
        }

        pub fn addItem(self: *Self, allocator: std.mem.Allocator, item: T) !void {
            try self.list.append(allocator, item);
            errdefer _ = self.list.pop();

            std.mem.sort(u64, self.list.items, {}, std.sort.asc(u64));
        }

        pub fn removeItem(self: *Self, item: T) bool {
            for (self.list.items, 0..self.list.items.len) |k, index| {
                if (k == item) {
                    // NOTE: I'm like 99% sure that we don't need to resort the list at all if we do an ordered remove
                    //     i'm dumb though
                    _ = self.list.orderedRemove(index);
                    return true;
                }
            }

            return false;
        }

        pub fn next(self: *Self) ?T {
            if (self.list.items.len == 0) return null;

            self.current_index = @min(self.list.items.len - 1, self.current_index);
            const item = self.list.items[self.current_index];

            self.current_index = (self.current_index + 1) % self.list.items.len;

            return item;
        }
    };
}
