const std = @import("std");

const Connection = @import("../protocol/connection.zig").Connection;

pub const ConnectionHandle = struct {
    const Self = @This();

    connection: *Connection,
};
