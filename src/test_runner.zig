const std = @import("std");
const testing = std.testing;

pub fn main() !void {
    const args = try std.process.argsAlloc(std.heap.page_allocator);

    const verbose = for (args) |arg| {
        if (std.mem.eql(u8, arg, "--verbose")) break true;
    } else false;

    if (verbose) testing.enableVerboseOutput();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    try testing.runTests(allocator, .{});
}
