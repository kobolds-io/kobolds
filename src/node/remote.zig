const std = @import("std");

const Connection = @import("../protocol/connection.zig").Connection;

pub const Remote = struct {
    const Self = @This();

    connections: std.ArrayList(*Connection),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .connections = std.ArrayList(*Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.connections.deinit();
    }

    // pub fn ping(self: *Self)
};
