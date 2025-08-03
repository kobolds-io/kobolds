const std = @import("std");

pub const ServiceLoadBalancer = union(ServiceLoadBalancingStrategy) {
    round_robin: RoundRobinLoadBalancer,
};

pub const ServiceLoadBalancingStrategy = enum {
    round_robin,
};

pub const RoundRobinLoadBalancer = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    current_index: usize,
    keys: std.ArrayList(u128),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .current_index = 0,
            .keys = std.ArrayList(u128).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.keys.deinit();
    }
};
