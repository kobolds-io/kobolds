const std = @import("std");
const uuid = @import("uuid");

const Connection = @import("../protocol/connection.zig").Connection;

pub const Remote = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    id: uuid.Uuid,

    pub fn init(allocator: std.mem.Allocator, id: uuid.Uuid) Self {
        return Self{
            .allocator = allocator,
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .id = id,
        };
    }

    pub fn deinit(self: *Self) void {
        self.connections.deinit();
    }

    // pub fn send(self: *Self, message: *Message) !void {
    //     if (self.connections.items.len == 0) {
    //         message.deref();
    //     }
    // }
};
