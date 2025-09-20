const std = @import("std");
const clap = @import("clap");

const RootCommand = @import("./cli/root.zig").RootCommand;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var iter = try std.process.ArgIterator.initWithAllocator(allocator);
    defer iter.deinit();

    try RootCommand(allocator, &iter);
}
