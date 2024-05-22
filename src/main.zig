const std = @import("std");
const parser = @import("./parser.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    errdefer _ = gpa.deinit();

    // 05hello
    const raw_bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    const bytes_mem = std.mem.sliceAsBytes(raw_bytes[0..4]);
    var stream = std.io.fixedBufferStream(bytes_mem);

    // this is the length of the
    const result = parser.readU32sFromReader(allocator, stream.reader()) catch |err| {
        std.debug.print("Error: Invalid byte array length {any}\n", .{err});
        return;
    };

    std.debug.print("The u32 value is: {any}\n", .{result.items});
}
