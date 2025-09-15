const std = @import("std");
const zbench = @import("zbench");

test "prints system info" {
    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("--------------------------------------------------------\n");
    try writer.print("{f}", .{try zbench.getSystemInfo()});
    try writer.writeAll("--------------------------------------------------------\n");
}

comptime {
    _ = @import("./benchmarks/checksum.zig");
    _ = @import("./benchmarks/message2.zig");
    _ = @import("./benchmarks/message.zig");
    _ = @import("./benchmarks/parser2.zig");
    _ = @import("./benchmarks/parser.zig");
}
