const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Message = @import("../protocol/message.zig").Message;

const constants = @import("../constants.zig");

pub fn BenchmarkMessageEncode(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    message.encode(backing_buf[0..message.size()]);
    // std.debug.print("encoded message {any}", .{backing_buf[0..message.size()]});
}

pub fn BenchmarkMessageDecode(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const encoded_message = ([_]u8{ 185, 109, 74, 197, 189, 38, 87, 150, 98, 99, 131, 40, 248, 184, 217, 148, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100 } ++ body);
    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

pub fn BenchmarkMessageCompressGzip(_: std.mem.Allocator) void {
    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.headers.compression = .gzip;
    message.headers.compressed = false;
    message.setBody(body);

    message.compress() catch unreachable;
}

pub fn BenchmarkMessageDecompressGzip(_: std.mem.Allocator) void {
    // NOTE: this body is "a" ** constants.message_max_body_size but compressed with gzip
    const body = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 237, 192, 129, 12, 0, 0, 0, 195, 48, 214, 249, 75, 156, 227, 73, 91, 0, 0, 0, 0, 0, 0, 0, 192, 187, 1, 213, 102, 111, 13, 0, 32, 0, 0 };
    var message = Message.new();
    message.headers.compression = .gzip;
    message.headers.compressed = true;
    message.setBody(&body);

    message.decompress() catch unreachable;
}

test "Message benchmarks" {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = 1 });
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    // try bench.add("encode", BenchmarkMessageEncode, .{});
    try bench.add("decode", BenchmarkMessageDecode, .{});
    // try bench.add("compress gzip", BenchmarkMessageCompressGzip, .{});
    // try bench.add("decompress gzip", BenchmarkMessageDecompressGzip, .{});

    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|--------------------|\n");
    try stderr.writeAll("| Message Benchmarks |\n");
    try stderr.writeAll("|--------------------|\n");
    try bench.run(stderr);
}
