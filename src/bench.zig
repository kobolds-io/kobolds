const std = @import("std");
const zbench = @import("zbench");

test "prints system info" {
    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("--------------------------------------------------------\n");
    try stderr.print("{}", .{try zbench.getSystemInfo()});
    try stderr.writeAll("--------------------------------------------------------\n");
}

comptime {
    _ = @import("./benchmarks/checksum.zig");
    _ = @import("./benchmarks/message.zig");
    _ = @import("./benchmarks/parser.zig");
}
