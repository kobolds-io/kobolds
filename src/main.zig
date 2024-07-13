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
    var bytes_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const bytes_allocator = bytes_gpa.allocator();
    defer _ = bytes_gpa.deinit();

    var bytes = std.ArrayList(u8).init(bytes_allocator);
    defer bytes.deinit();

    try original_msg.cborStringify(.{}, bytes.writer());

    // generate a length prefixed payload
    try bytes.insertSlice(0, &utils.u32ToBytes(@intCast(bytes.items.len)));

    std.debug.print("bytes.items.len {any}\n", .{bytes.items.len});
    std.debug.print("bytes.items {any}\n", .{bytes.items});
    // parse the length prefixed payload
    // deserialize the message
    // verify that the message is the same
}
