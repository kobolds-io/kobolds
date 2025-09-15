const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");
const testing = std.testing;

const KID = @import("../protocol/kid.zig").KID;

pub fn BenchmarkKIDGenerateOne(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    _ = kid.generate();
}

pub fn BenchmarkKIDGenerate1000(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    for (0..1_000) |_| {
        _ = kid.generate();
    }
}

pub fn BenchmarkKIDGenerate8192(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    for (0..8192) |_| {
        _ = kid.generate();
    }
}

pub fn BenchmarkKIDGenerate10000(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    for (0..10_000) |_| {
        _ = kid.generate();
    }
}

pub fn BenchmarkKIDGenerate100000(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    for (0..100_000) |_| {
        _ = kid.generate();
    }
}

pub fn BenchmarkKIDGenerate1000000(_: std.mem.Allocator) void {
    var kid = KID.init(100, .{});
    for (0..1_000_000) |_| {
        _ = kid.generate();
    }
}

test "KID benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        // .iterations = benchmark_constants.benchmark_max_iterations,
        .iterations = 100,
    });
    defer bench.deinit();

    try bench.add("generate 1 ids", BenchmarkKIDGenerateOne, .{});
    try bench.add("generate 1000 ids", BenchmarkKIDGenerate1000, .{});
    try bench.add("generate 8192 ids", BenchmarkKIDGenerate8192, .{});
    try bench.add("generate 10_000 ids", BenchmarkKIDGenerate10000, .{});
    try bench.add("generate 100_000 ids", BenchmarkKIDGenerate100000, .{});
    try bench.add("generate 1_000_000 ids", BenchmarkKIDGenerate1000000, .{ .iterations = 5 });

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|----------------|\n");
    try writer.writeAll("| KID Benchmarks |\n");
    try writer.writeAll("|----------------|\n");
    try bench.run(writer);
}
