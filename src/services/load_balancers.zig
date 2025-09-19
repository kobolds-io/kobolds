const std = @import("std");

pub fn RoundRobinLoadBalancer(comptime T: type) type {
    return struct {
        const Self = @This();

        current_index: usize,
        keys: std.ArrayList(T),

        pub fn init() Self {
            return Self{
                .current_index = 0,
                .keys = .empty,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.keys.deinit(allocator);
        }
    };
}
