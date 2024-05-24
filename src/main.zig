const std = @import("std");
const MessageParser = @import("./parser.zig").MessageParser;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    errdefer _ = gpa.deinit();

    var parser = MessageParser.init(allocator);

    // 05hello07hellooo
    const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    // 05hello
    // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    var timer = try std.time.Timer.start();

    const iters = 1_000_000;
    std.debug.print("sending {} bytes through parser {} times\n", .{ bytes.len, iters });
    for (0..iters) |_| {
        _ = try parser.parse(&bytes);
    }

    const took = timer.read();
    std.debug.print("took {}ns\n", .{took});
    std.debug.print("took {}us\n", .{took / std.time.ns_per_us});
    std.debug.print("took {}ms\n", .{took / std.time.ns_per_ms});
}
