const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const LoadBalancer = @import("../load_balancers/load_balancer.zig").LoadBalancer;
const LoadBalancerStrategy = @import("../load_balancers/load_balancer.zig").LoadBalancerStrategy;
const Connection = @import("../protocol/connection2.zig").Connection;

pub const ConnectionPool = struct {
    const Self = @This();

    pub const ConnectionPoolConfig = struct {
        max_connections: u8,
        min_connections: u8,
    };

    config: ConnectionPoolConfig,
    connections: std.AutoHashMapUnmanaged(u64, *Connection),
    load_balancer: LoadBalancer(u64),

    pub fn init(
        config: ConnectionPoolConfig,
        load_balancing_strategy: LoadBalancerStrategy,
    ) Self {
        return switch (load_balancing_strategy) {
            .round_robin => Self{
                .config = config,
                .connections = .empty,
                .load_balancer = LoadBalancer(u64){
                    .round_robin = .init(),
                },
            },
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.connections.deinit(allocator);
        switch (self.load_balancer) {
            .round_robin => |*lb| lb.deinit(allocator),
        }
    }

    pub fn addConnection(self: *Self, allocator: std.mem.Allocator, conn: *Connection) !void {
        if (self.connections.get(conn.connection_id)) |_| {
            return error.AlreadyExists;
        }

        try self.connections.put(allocator, conn.connection_id, conn);
        errdefer _ = self.connections.remove(conn.connection_id);

        switch (self.load_balancer) {
            .round_robin => |*lb| try lb.addItem(allocator, conn.connection_id),
        }
    }

    pub fn removeConnection(self: *Self, conn_id: u64) bool {
        const lb_removed = switch (self.load_balancer) {
            .round_robin => |*lb| lb.removeItem(conn_id),
        };

        return self.connections.remove(conn_id) and lb_removed;
    }

    pub fn getNextConnection(self: *Self) *Connection {
        assert(self.connections.count() > 0);

        const lb_next = switch (self.load_balancer) {
            .round_robin => |lb| lb.next(),
        };

        if (lb_next) |conn_id| {
            return self.connections.get(conn_id).?;
        } else @panic("self.connections and load_balancer.items do not match");
        // NOTE: this means that we are out of sync and have no idea how to deal with this
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    const config = ConnectionPool.ConnectionPoolConfig{
        .max_connections = 100,
        .min_connections = 1,
    };

    var connection_pool = ConnectionPool.init(config, .round_robin);
    defer connection_pool.deinit(allocator);
}
