const std = @import("std");

const RoundRobin = @import("./round_robin.zig").RoundRobin;

pub fn LoadBalancer(comptime T: type) type {
    return union(LoadBalancerStrategy) {
        round_robin: RoundRobin(T),
    };
}

pub const LoadBalancerStrategy = enum {
    round_robin,
};
