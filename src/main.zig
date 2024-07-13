const std = @import("std");
const net = std.net;
const Node = @import("./proto/node.zig").Node;
const MessageParser = @import("./proto/parser.zig").MessageParser;
const Message = @import("./proto/message.zig").Message;
const MessageType = @import("./proto/message.zig").MessageType;
const utils = @import("./proto/utils.zig");

pub fn main() !void {
    // create a message
    const original_msg = Message.new("1", "/hello", "world");

    // serialize it
    var buffer_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const buf_allocator = buffer_gpa.allocator();
    defer _ = buffer_gpa.deinit();

    var buf = std.ArrayList(u8).init(buf_allocator);
    defer buf.deinit();

    try utils.serialize(&buf, original_msg);

    std.debug.print("bytes.items.len {any}\n", .{buf.items.len});
    std.debug.print("bytes.items {any}\n", .{buf.items});
    // parse the length prefixed payload
    // deserialize the message
    // verify that the message is the same
}
