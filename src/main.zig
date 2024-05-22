const std = @import("std");
const MessageParser = @import("./parser.zig").MessageParser;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    errdefer _ = gpa.deinit();

    var parser = MessageParser.init(allocator);

    // 05hello07hellooo
    const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    const msgs = try parser.parse(allocator, &bytes);

    for (msgs) |msg| {
        std.debug.print("Message! {any}\n", .{msg});
    }

    // const bytes_mem = std.mem.sliceAsBytes(raw_bytes[0..4]);
    // var stream = std.io.fixedBufferStream(bytes_mem);
    //
    // // this is the length of the
    // const num = parser.beToU32(allocator, stream.reader()) catch |err| {
    //     std.debug.print("Error: Invalid byte array length {any}\n", .{err});
    //     return;
    // };

    // std.debug.print("The u32 value is: {any}\n", .{num});
}
