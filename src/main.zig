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
    var serialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const serialize_buf_allocator = serialize_buf_gpa.allocator();
    defer _ = serialize_buf_gpa.deinit();

    var serialize_buf = std.ArrayList(u8).init(serialize_buf_allocator);
    defer serialize_buf.deinit();

    try utils.serialize(&serialize_buf, original_msg);

    var deserialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const deserialize_buf_allocator = deserialize_buf_gpa.allocator();
    // defer _ = deserialize_buf_gpa.deinit();

    const deserialized_msg = try utils.deserialize(
        deserialize_buf_allocator,
        serialize_buf.items[4..],
    );

    std.debug.print("original msg {any}\n", .{original_msg});
    std.debug.print("deserialized msg {any}\n", .{deserialized_msg});
    // parse the length prefixed payload
    // deserialize the message
    // verify that the message is the same
}
