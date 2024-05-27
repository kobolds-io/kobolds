const std = @import("std");
const Message = @import("./message.zig");

const Connection = struct {
    id: []const u8,
    conn: *std.net.Server.Connection,
};

/// A node is the primary building block of the system. Nodes handle the async
/// read and write loops that allow messages to be transmitted from node to node.
pub const Node = struct {
    const Self = @This();
    connections: std.StringHashMap(Connection) = undefined,

    pub fn new(allocator: std.mem.Allocator) Node {
        return Node{
            .connections = std.StringHashMap(Connection).init(allocator),
        };
    }

    pub fn handle_connection(node: *Node, server_conn: *std.net.Server.Connection) void {
        defer server_conn.stream.close();
        const connection = Connection{
            .id = "some id",
            .conn = server_conn,
        };

        // TODO: there needs to be a mutex put on this for race conditions/collisions
        node.connections.put("some id", connection) catch |err| {
            std.debug.print("could not add connection {}\n", .{err});
            return;
        };
        defer _ = node.connections.remove(connection.id);

        // TODO: spawn a read loop task

        // TODO: spawn a write loop task

        std.debug.print("node.handle_connection: connection {any}\n", .{connection});
    }
};
