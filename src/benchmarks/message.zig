const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Message = @import("../protocol/message2.zig").Message;

const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");

pub fn BenchmarkMessageSerializeLarge(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new(.undefined);
    message.setBody(body);

    const bytes = message.serialize(backing_buf[0..message.size()]);
    // _ = message.serialize2(backing_buf[0..message.size()]);

    _ = bytes;
    // std.debug.print("serialized message {any}", .{backing_buf[0..bytes]});
}

pub fn BenchmarkMessageDeserializeLarge(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const checksum = [_]u8{ 44, 70, 157, 175, 168, 53, 152, 96 };
    // const checksum = [_]u8{ 253, 10, 237, 227 };
    const encoded_message = ([_]u8{ 32, 0, 0, 0, 0, 0 } ++ body ++ checksum);
    _ = Message.deserialize(&encoded_message) catch unreachable;
}

pub fn BenchmarkMessageSerializeSmall(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    var message = Message.new(.undefined);

    const bytes = message.serialize(backing_buf[0..message.size()]);
    // _ = message.serialize2(backing_buf[0..message.size()]);

    _ = bytes;
    // std.debug.print("serialized message {any}", .{backing_buf[0..bytes]});
}

pub fn BenchmarkMessageDeserializeSmall(_: std.mem.Allocator) void {
    const checksum = [_]u8{ 58, 247, 81, 79, 183, 108, 92, 217 };
    // const checksum = [_]u8{ 177, 194, 161, 163 };
    const encoded_message = ([_]u8{ 0, 0, 0, 0, 0, 0 } ++ checksum);
    _ = Message.deserialize(&encoded_message) catch unreachable;
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

test "Message2 benchmarks" {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = 1 });
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("serialize large", BenchmarkMessageSerializeLarge, .{});
    try bench.add("deserialize large", BenchmarkMessageDeserializeLarge, .{});
    try bench.add("serialize small", BenchmarkMessageSerializeSmall, .{});
    try bench.add("deserialize small", BenchmarkMessageDeserializeSmall, .{});
    // try bench.add("compress gzip", BenchmarkMessageCompressGzip, .{});
    // try bench.add("decompress gzip", BenchmarkMessageDecompressGzip, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|---------------------|\n");
    try writer.writeAll("| Message2 Benchmarks |\n");
    try writer.writeAll("|---------------------|\n");
    try bench.run(writer);
}
