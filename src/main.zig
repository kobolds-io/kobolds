const std = @import("std");
const app = @import("./cli/app.zig");

pub fn main() !void {
    try app.run();
}
