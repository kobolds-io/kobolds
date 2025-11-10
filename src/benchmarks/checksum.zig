const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const benchmark_constants = @import("./constants.zig");
const testing = std.testing;

const check = @import("../lib/checksum.zig");

pub fn BenchmarkXxHash32Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = check.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 2876315459;

    assert(check.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash32Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u32 = 1092953798;

    assert(check.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = check.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u64 = 10140113291757988233;

    assert(check.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u64 = 14436252957693351467;

    assert(check.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkCRC32Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = check.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 1386227895;

    assert(check.verifyCrc32(sum, &test_bytes));
}

pub fn BenchmarkCRC32Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u32 = 890172475;

    assert(check.verifyCrc32(sum, &test_bytes));
}

test "checksum benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("xxhash32 checksum 16 bytes", BenchmarkXxHash32Checksum16Bytes, .{});
    try bench.add("xxhash32 checksum 128 bytes", BenchmarkXxHash32Checksum128Bytes, .{});
    try bench.add("xxhash64 checksum 16 bytes", BenchmarkXxHash64Checksum16Bytes, .{});
    try bench.add("xxhash64 checksum 128 bytes", BenchmarkXxHash64Checksum128Bytes, .{});
    try bench.add("crc32 checksum 16 bytes", BenchmarkCRC32Checksum16Bytes, .{});
    try bench.add("crc32 checksum 128 bytes", BenchmarkCRC32Checksum128Bytes, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|---------------------|\n");
    try writer.writeAll("| Checksum Benchmarks |\n");
    try writer.writeAll("|---------------------|\n");
    try bench.run(writer);
}

test "verify benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("xxhash32 verify 16 bytes", BenchmarkXxHash32Verify16Bytes, .{});
    try bench.add("xxhash32 verify 128 bytes", BenchmarkXxHash32Verify128Bytes, .{});
    try bench.add("xxhash64 verify 16 bytes", BenchmarkXxHash64Verify16Bytes, .{});
    try bench.add("xxhash64 verify 128 bytes", BenchmarkXxHash64Verify128Bytes, .{});
    try bench.add("crc32 verify 16 bytes", BenchmarkCRC32Verify16Bytes, .{});
    try bench.add("crc32 verify 128 bytes", BenchmarkCRC32Verify128Bytes, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|----------------------------|\n");
    try writer.writeAll("| Checksum Verify Benchmarks |\n");
    try writer.writeAll("|----------------------------|\n");
    try bench.run(writer);
}
