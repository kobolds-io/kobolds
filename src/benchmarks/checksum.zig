const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const benchmark_constants = @import("./constants.zig");
const testing = std.testing;

const check = @import("../lib/checksum.zig");

pub fn BenchmarkXxHash32ChecksumTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32ChecksumMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);

    _ = check.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32ChecksumLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);

    _ = check.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32VerifyTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 2876315459;

    assert(check.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash32VerifyMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);
    const sum: u32 = 2186680621;

    assert(check.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash32VerifyLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);
    const sum: u32 = 871297165;

    assert(check.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64ChecksumTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64ChecksumMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);

    _ = check.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64ChecksumLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);

    _ = check.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64VerifyTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u64 = 10140113291757988233;

    assert(check.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64VerifyMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);
    const sum: u64 = 3581703012154252652;

    assert(check.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64VerifyLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);
    const sum: u64 = 7904164552034802651;

    assert(check.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkCRC32ChecksumTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = check.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32ChecksumMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);

    _ = check.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32ChecksumLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);

    _ = check.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32VerifyTinyBytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 1386227895;

    assert(check.verifyCrc32(sum, &test_bytes));
}

pub fn BenchmarkCRC32VerifyMediumBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u8)]u8 = [_]u8{1} ** std.math.maxInt(u8);
    const sum: u32 = 1444046329;

    assert(check.verifyCrc32(sum, &test_bytes));
}

pub fn BenchmarkCRC32VerifyLargeBytes(_: std.mem.Allocator) void {
    const test_bytes: [std.math.maxInt(u16)]u8 = [_]u8{1} ** std.math.maxInt(u16);
    const sum: u32 = 1040761532;

    assert(check.verifyCrc32(sum, &test_bytes));
}

test "checksum benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    try bench.add("xxhash32 tiny bytes", BenchmarkXxHash32ChecksumTinyBytes, .{});
    try bench.add("xxhash32 medium bytes", BenchmarkXxHash32ChecksumMediumBytes, .{});
    try bench.add("xxhash32 large bytes", BenchmarkXxHash32ChecksumLargeBytes, .{});
    try bench.add("xxhash64 tiny bytes", BenchmarkXxHash64ChecksumTinyBytes, .{});
    try bench.add("xxhash64 medium bytes", BenchmarkXxHash64ChecksumMediumBytes, .{});
    try bench.add("xxhash64 large bytes", BenchmarkXxHash64ChecksumLargeBytes, .{});
    try bench.add("crc32 tiny bytes", BenchmarkCRC32ChecksumTinyBytes, .{});
    try bench.add("crc32 medium bytes", BenchmarkCRC32ChecksumMediumBytes, .{});
    try bench.add("crc32 large bytes", BenchmarkCRC32ChecksumLargeBytes, .{});

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

    try bench.add("xxhash32 tiny bytes", BenchmarkXxHash32VerifyTinyBytes, .{});
    try bench.add("xxhash32 medium bytes", BenchmarkXxHash32VerifyMediumBytes, .{});
    try bench.add("xxhash32 large bytes", BenchmarkXxHash32VerifyLargeBytes, .{});
    try bench.add("xxhash64 tiny bytes", BenchmarkXxHash64VerifyTinyBytes, .{});
    try bench.add("xxhash64 medium bytes", BenchmarkXxHash64VerifyMediumBytes, .{});
    try bench.add("xxhash64 large bytes", BenchmarkXxHash64VerifyLargeBytes, .{});
    try bench.add("crc32 tiny bytes", BenchmarkCRC32VerifyTinyBytes, .{});
    try bench.add("crc32 medium bytes", BenchmarkCRC32VerifyMediumBytes, .{});
    try bench.add("crc32 large bytes", BenchmarkCRC32VerifyLargeBytes, .{});

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|----------------------------|\n");
    try writer.writeAll("| Checksum Verify Benchmarks |\n");
    try writer.writeAll("|----------------------------|\n");
    try bench.run(writer);
}
