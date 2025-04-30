const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");
const testing = std.testing;

const hash = @import("../hash.zig");

pub fn BenchmarkXxHash32Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = hash.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = hash.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32ChecksumMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;

    _ = hash.xxHash32Checksum(&test_bytes);
}

pub fn BenchmarkXxHash32Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 2876315459;

    assert(hash.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash32Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u32 = 1092953798;

    assert(hash.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash32VerifyMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;
    const sum: u32 = 3540356943;

    assert(hash.xxHash32Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = hash.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = hash.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64ChecksumMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;

    _ = hash.xxHash64Checksum(&test_bytes);
}

pub fn BenchmarkXxHash64Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u64 = 10140113291757988233;

    assert(hash.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u64 = 14436252957693351467;

    assert(hash.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkXxHash64VerifyMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;
    const sum: u64 = 16728056813270301098;

    assert(hash.xxHash64Verify(sum, &test_bytes));
}

pub fn BenchmarkCRC32Checksum16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;

    _ = hash.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32Checksum128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;

    _ = hash.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32ChecksumMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;

    _ = hash.checksumCrc32(&test_bytes);
}

pub fn BenchmarkCRC32Verify16Bytes(_: std.mem.Allocator) void {
    const test_bytes: [16]u8 = [_]u8{1} ** 16;
    const sum: u32 = 1386227895;

    assert(hash.verifyCrc32(sum, &test_bytes));
}

pub fn BenchmarkCRC32Verify128Bytes(_: std.mem.Allocator) void {
    const test_bytes: [128]u8 = [_]u8{1} ** 128;
    const sum: u32 = 890172475;

    assert(hash.verifyCrc32(sum, &test_bytes));
}

pub fn BenchmarkCRC32VerifyMessageBody(_: std.mem.Allocator) void {
    const test_bytes: [constants.message_max_body_size]u8 = [_]u8{1} ** constants.message_max_body_size;
    const sum: u32 = 1286701566;

    assert(hash.verifyCrc32(sum, &test_bytes));
}

test "checksum benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    try bench.add("xxhash32 checksum 16 bytes", BenchmarkXxHash32Checksum16Bytes, .{});
    try bench.add("xxhash32 checksum 128 bytes", BenchmarkXxHash32Checksum128Bytes, .{});
    try bench.add("xxhash32 checksum message body", BenchmarkXxHash32ChecksumMessageBody, .{});
    try bench.add("xxhash64 checksum 16 bytes", BenchmarkXxHash64Checksum16Bytes, .{});
    try bench.add("xxhash64 checksum 128 bytes", BenchmarkXxHash64Checksum128Bytes, .{});
    try bench.add("xxhash64 checksum message body", BenchmarkXxHash64ChecksumMessageBody, .{});
    try bench.add("crc32 checksum 16 bytes", BenchmarkCRC32Checksum16Bytes, .{});
    try bench.add("crc32 checksum 128 bytes", BenchmarkCRC32Checksum128Bytes, .{});
    try bench.add("crc32 checksum message body", BenchmarkCRC32ChecksumMessageBody, .{});

    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|---------------------|\n");
    try stderr.writeAll("| checksum Benchmarks |\n");
    try stderr.writeAll("|---------------------|\n");
    try bench.run(stderr);
}

test "verify benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    try bench.add("xxhash32 verify 16 bytes", BenchmarkXxHash32Verify16Bytes, .{});
    try bench.add("xxhash32 verify 128 bytes", BenchmarkXxHash32Verify128Bytes, .{});
    try bench.add("xxhash32 verify message body", BenchmarkXxHash32VerifyMessageBody, .{});
    try bench.add("xxhash64 verify 16 bytes", BenchmarkXxHash64Verify16Bytes, .{});
    try bench.add("xxhash64 verify 128 bytes", BenchmarkXxHash64Verify128Bytes, .{});
    try bench.add("xxhash64 verify message body", BenchmarkXxHash64VerifyMessageBody, .{});
    try bench.add("crc32 verify 16 bytes", BenchmarkCRC32Verify16Bytes, .{});
    try bench.add("crc32 verify 128 bytes", BenchmarkCRC32Verify128Bytes, .{});
    try bench.add("crc32 verify message body", BenchmarkCRC32VerifyMessageBody, .{});

    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|-------------------|\n");
    try stderr.writeAll("| verify Benchmarks |\n");
    try stderr.writeAll("|-------------------|\n");
    try bench.run(stderr);
}
