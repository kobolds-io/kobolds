const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Message = @import("../protocol/message.zig").Message;

const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");

pub fn BenchmarkMessageEncodeLarge(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    message.encode(backing_buf[0..message.size()]);
    // std.debug.print("encoded message {any}", .{backing_buf[0..message.size()]});
}

pub fn BenchmarkMessageDecodeLarge(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const encoded_message = ([_]u8{ 215, 38, 232, 118, 41, 51, 190, 40, 98, 99, 131, 40, 248, 184, 217, 148, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);
    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

pub fn BenchmarkMessageEncodeSmall(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    var message = Message.new();

    message.encode(backing_buf[0..message.size()]);

    // std.debug.print("serialized message {any}", .{backing_buf[0..message.size()]});
}

pub fn BenchmarkMessageDecodeSmall(_: std.mem.Allocator) void {
    const encoded_message = ([_]u8{ 174, 244, 12, 190, 130, 54, 243, 42, 169, 132, 152, 166, 133, 192, 119, 150, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

// pub fn BenchmarkMessageCompressGzip(_: std.mem.Allocator) void {
//     const body = comptime "a" ** constants.message_max_body_size;
//     var message = Message.new();
//     message.headers.compression = .gzip;
//     message.headers.compressed = false;
//     message.setBody(body);

//     message.compress() catch unreachable;
// }

// pub fn BenchmarkMessageDecompressGzip(_: std.mem.Allocator) void {
//     // NOTE: this body is "a" ** constants.message_max_body_size but compressed with gzip
//     const body = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 237, 192, 129, 12, 0, 0, 0, 195, 48, 214, 249, 75, 156, 227, 73, 91, 0, 0, 0, 0, 0, 0, 0, 192, 187, 1, 213, 102, 111, 13, 0, 32, 0, 0 };
//     var message = Message.new();
//     message.headers.compression = .gzip;
//     message.headers.compressed = true;
//     message.setBody(&body);

//     message.decompress() catch unreachable;
// }

test "Message benchmarks" {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = 1 });
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("encode large", BenchmarkMessageEncodeLarge, .{});
    try bench.add("decode large", BenchmarkMessageDecodeLarge, .{});
    try bench.add("encode small", BenchmarkMessageEncodeSmall, .{});
    try bench.add("decode small", BenchmarkMessageDecodeSmall, .{});
    // try bench.add("compress gzip", BenchmarkMessageCompressGzip, .{});
    // try bench.add("decompress gzip", BenchmarkMessageDecompressGzip, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|--------------------|\n");
    try writer.writeAll("| Message Benchmarks |\n");
    try writer.writeAll("|--------------------|\n");
    try bench.run(writer);
}
