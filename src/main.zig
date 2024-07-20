const std = @import("std");
const Cluster = @import("./proto/cluster.zig").Cluster;

pub fn main() !void {
    std.debug.print("hello from harpy\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var cluster = try Cluster.init(allocator, .{});
    defer cluster.deinit();

    try cluster.listen();
}
