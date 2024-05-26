const std = @import("std");
const Message = @import("./message.zig");

const Connection = struct {};

/// A node is the primary building block of the system. Nodes handle the async
/// read and write loops that allow messages to be transmitted from node to node.
pub const Node = struct {
    const Self = @This();

    inbox: std.ArrayList(Message),
    outbox: std.ArrayList(Message),
    connections: std.StringHashMap(Connection),

    pub fn new(inbox_allocator: std.mem.Allocator, outbox_allocator: std.mem.Allocator, connections_allocator: std.mem.Allocator) Node {
        return Node{
            .inbox = std.ArrayList(Message).init(inbox_allocator),
            .outbox = std.ArrayList(Message).init(outbox_allocator),
            .connections = std.StringHashMap(Connection).init(connections_allocator),
        };
    }

    pub fn new_shared(allocator: std.mem.Allocator) Node {
        return Node{
            .inbox = std.ArrayList(Message).init(allocator),
            .outbox = std.ArrayList(Message).init(allocator),
            .connections = std.StringHashMap(Message).init(allocator),
        };
    }

    // pub fn add_connection(id: []const u8, )
};
