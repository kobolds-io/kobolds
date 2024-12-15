const std = @import("std");

pub fn Mailbox(comptime T: type) type {
    return struct {
        const Self = @This();

        list: std.ArrayList(T),
        mutex: std.Thread.Mutex,

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .list = std.ArrayList(T).init(allocator),
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.list.deinit();
        }

        pub fn append(self: *Self, item: T) !void {
            try self.list.append(item);
        }

        // This is a slow an inefficient function due to the data structure used. A linked list would be pretty good but
        // i have to implement one that can be allocated. Another option is to use a circular buffer but again,
        // i have to implement it.
        pub fn popHead(self: *Self) ?T {
            // remove the first item from the list
            if (self.list.items.len == 0) return null;
            if (self.list.items.len == 1) return self.list.pop();

            // remove the first item out of the array
            const item = self.list.items[0];
            const n = 1;

            std.mem.copyForwards(T, self.list.items, self.list.items[n..]);
            self.list.items.len -= n;

            return item;
        }
    };
}
