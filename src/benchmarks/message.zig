const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Message = @import("../protocol/message.zig").Message;

const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");

pub fn BenchmarkMessageSerializeLarge(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new(.publish);
    message.setBody(body);

    _ = message.serialize(backing_buf[0..message.size()]);

    // uncomment to print message
    // const bytes = message.serialize(backing_buf[0..message.size()]);
    // std.debug.print("serialized message {any}", .{backing_buf[0..bytes]});
}

pub fn BenchmarkMessageDeserializeLarge(_: std.mem.Allocator) void {
    const fixed_headers = [_]u8{ 32, 0, 6, 16, 0, 0, 0 };
    const body = [_]u8{97} ** constants.message_max_body_size;
    const checksum = [_]u8{ 246, 117, 61, 108, 66, 225, 20, 31 };
    const encoded_message = [_]u8{} ++ fixed_headers ++ body ++ checksum;
    _ = Message.deserialize(&encoded_message) catch unreachable;
}

pub fn BenchmarkMessageSerializeSmall(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    var message = Message.new(.publish);

    _ = message.serialize(backing_buf[0..message.size()]);

    // uncomment to print message
    // const bytes = message.serialize(backing_buf[0..message.size()]);
    // std.debug.print("serialized message {any}", .{backing_buf[0..bytes]});
}

pub fn BenchmarkMessageDeserializeSmall(_: std.mem.Allocator) void {
    const fixed_headers = [_]u8{ 0, 0, 6, 16, 0, 0, 0 };
    const body = [_]u8{};
    const checksum = [_]u8{ 42, 194, 42, 98, 208, 35, 219, 28 };
    const encoded_message = ([_]u8{} ++ fixed_headers ++ body ++ checksum);
    _ = Message.deserialize(&encoded_message) catch unreachable;
}

test "Message benchmarks" {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = 1 });
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("serialize large", BenchmarkMessageSerializeLarge, .{});
    try bench.add("deserialize large", BenchmarkMessageDeserializeLarge, .{});
    try bench.add("serialize small", BenchmarkMessageSerializeSmall, .{});
    try bench.add("deserialize small", BenchmarkMessageDeserializeSmall, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|--------------------|\n");
    try writer.writeAll("| Message Benchmarks |\n");
    try writer.writeAll("|--------------------|\n");
    try bench.run(writer);
}
