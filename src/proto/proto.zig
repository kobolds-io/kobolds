const std = @import("std");
const Message = @import("./message.zig").Message;
const serialize = @import("./utils.zig").serialize;

// The purpose of this test is to have an automated enforcement
// for the serialization of messages
test "benchmark message serialization" {
    // Create an empty default message on the stack
    const msg = Message.new("", "", "");

    /////////////////// SERIALIZE THE MESSAGE
    var serialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const serialize_buf_allocator = serialize_buf_gpa.allocator();
    defer _ = serialize_buf_gpa.deinit();

    var serialize_buf = std.ArrayList(u8).init(serialize_buf_allocator);
    defer serialize_buf.deinit();

    var serialize_timer = try std.time.Timer.start();
    defer serialize_timer.reset();

    try serialize(&serialize_buf, msg);

    const serialize_duration = serialize_timer.read();

    std.debug.print("serialize took {}us\n", .{(serialize_duration / std.time.ns_per_us)});
}
