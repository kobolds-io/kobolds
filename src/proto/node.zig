const std = @import("std");
const Message = @import("./message.zig").Message;
const MessageParser = @import("./parser.zig").MessageParser;

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
        std.debug.print("opened connection: connection {s}\n", .{connection.id});
        defer std.debug.print("closing connection: connection {s}\n", .{connection.id});

        // Create an allocator for the parser
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const allocator = gpa.allocator();
        // TODO: investigate why i get a memory leak detected if i return early
        //      is this ok that i leave the gpa alive? do i need to clean it?
        // defer _ = gpa.deinit();

        var parser = MessageParser.init(allocator);
        defer parser.deinit();

        var read_buffer: [1024]u8 = undefined;

        var messages_list_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const messages_list_allocator = messages_list_gpa.allocator();

        var messages_list = std.ArrayList([]u8).init(messages_list_allocator);
        defer messages_list.deinit();

        while (true) {
            const bytes_read = connection.conn.stream.read(&read_buffer) catch |err| {
                std.debug.print("could not read from connection {}\n", .{err});
                return;
            };

            if (bytes_read == 0) {
                // end of file likely reached. Close the connection
                std.debug.print("could not read bytes from connection. EOF\n", .{});
                return;
            }

            std.debug.print("received bytes {any} \n", .{read_buffer[0..bytes_read]});

            const messages = parser.parse(&messages_list, read_buffer[0..bytes_read]) catch |err| {
                std.debug.print("could not parse message, {}\n", .{err});
                // clear out the read buffer
                read_buffer = undefined;
                continue;
            };

            if (messages_list.items.len > 0) {
                std.debug.print("messages: {any}\n", .{messages_list.items});
            }

            std.debug.print("messages {any} \n", .{messages});

            // clear out the read buffer
            read_buffer = undefined;
        }

        // TODO: spawn a write loop task

        // std.debug.print("node.handle_connection: connection {any}\n", .{connection});
    }
};
