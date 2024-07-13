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

    var serialize_timer = try std.time.Timer.start();
    defer serialize_timer.reset();

    // serialize it
    var serialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const serialize_buf_allocator = serialize_buf_gpa.allocator();
    defer _ = serialize_buf_gpa.deinit();

    var serialize_buf = std.ArrayList(u8).init(serialize_buf_allocator);
    defer serialize_buf.deinit();

    try utils.serialize(&serialize_buf, original_msg);

    const serialize_duration = serialize_timer.read();

    std.debug.print("serialize took {}us\n", .{(serialize_duration / std.time.ns_per_us)});

    var deserialize_timer = try std.time.Timer.start();
    defer deserialize_timer.reset();

    var deserialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const deserialize_buf_allocator = deserialize_buf_gpa.allocator();
    // defer _ = deserialize_buf_gpa.deinit();

    const deserialized_msg = try utils.deserialize(
        deserialize_buf_allocator,
        serialize_buf.items[4..],
    );

    const deserialize_duration = deserialize_timer.read();
    defer deserialize_timer.reset();

    std.debug.print("deserialize took {}us\n", .{deserialize_duration / std.time.ns_per_us});

    std.debug.print("original msg {any}\n", .{original_msg});
    std.debug.print("deserialized msg {any}\n", .{deserialized_msg});
}
