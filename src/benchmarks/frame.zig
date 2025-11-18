const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const benchmark_constants = @import("./constants.zig");
const testing = std.testing;

const Frame = @import("../lib/frame.zig").Frame;
const FrameParser = @import("../lib/frame.zig").FrameParser;
const Assembler = @import("../lib/frame.zig").Assembler;
const MemoryPool = @import("stdx").MemoryPool;
const Chunk = @import("../lib/chunk.zig").Chunk;

const check = @import("../lib/checksum.zig");

const FrameParseFrameFromBytes = struct {
    const Self = @This();

    frame_buf: []const u8,
    frame_parser: *FrameParser,
    pub fn new(frame_parser: *FrameParser, frame_buf: []const u8) Self {
        return Self{
            .frame_buf = frame_buf,
            .frame_parser = frame_parser,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        var frames: [1]Frame = undefined;

        const frames_parsed = self.frame_parser.parse(&frames, self.frame_buf) catch unreachable;
        assert(frames_parsed == 1);
    }
};

test "FrameParser benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    // allocate all the memory for the payloads
    const small_payload = allocator.alloc(u8, std.math.maxInt(u8)) catch unreachable;
    defer allocator.free(small_payload);

    const medium_payload = allocator.alloc(u8, std.math.maxInt(u16) / 2) catch unreachable;
    defer allocator.free(medium_payload);

    const large_payload = allocator.alloc(u8, std.math.maxInt(u16)) catch unreachable;
    defer allocator.free(large_payload);

    // make all the frames
    var small_frame = Frame.new(small_payload, .{});
    var medium_frame = Frame.new(medium_payload, .{});
    var large_frame = Frame.new(large_payload, .{});

    // create the frame_buffers
    const small_frame_buf = allocator.alloc(u8, small_frame.packedSize()) catch unreachable;
    defer allocator.free(small_frame_buf);

    const medium_frame_buf = allocator.alloc(u8, medium_frame.packedSize()) catch unreachable;
    defer allocator.free(medium_frame_buf);

    const large_frame_buf = allocator.alloc(u8, large_frame.packedSize()) catch unreachable;
    defer allocator.free(large_frame_buf);

    // convert all the frames to bytes
    _ = small_frame.toBytes(small_frame_buf);
    _ = medium_frame.toBytes(medium_frame_buf);
    _ = large_frame.toBytes(large_frame_buf);

    // -------------------------------------------------------------------------------------------
    // NOTE: It would be unrealistic to initialze a FrameParser within the benchmark because in
    // the real world, we would be reusing the same FrameParser. We would not be reallocating over
    // and over again. In this benchmark we would take the hit for the first allocation but not
    // pay for it after UNLESS the FrameParser/other decides to resize the buffer.
    // -------------------------------------------------------------------------------------------
    var frame_parser = FrameParser.init(allocator);
    defer frame_parser.deinit();

    try bench.addParam(
        "parse small frame",
        &FrameParseFrameFromBytes.new(&frame_parser, small_frame_buf),
        .{},
    );
    try bench.addParam(
        "parse medium frame",
        &FrameParseFrameFromBytes.new(&frame_parser, medium_frame_buf),
        .{},
    );
    try bench.addParam(
        "parse large frame",
        &FrameParseFrameFromBytes.new(&frame_parser, large_frame_buf),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|------------------------------|\n");
    try writer.writeAll("| FrameParser.parse Benchmarks |\n");
    try writer.writeAll("|------------------------------|\n");
    try bench.run(writer);
}

const AssembleFromFrames = struct {
    const Self = @This();

    assembler: *Assembler,
    frames: Frame,
    pool: *MemoryPool(Chunk),
    pub fn new(assembler: *Assembler, pool: *MemoryPool(Chunk), frames: Frame) Self {
        return Self{
            .assembler = assembler,
            .pool = pool,
            .frames = frames,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        _ = self;
    }
};

test "FrameAssembler benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    // TODO: create a benchmark for the assembler.

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|------------------------------|\n");
    try writer.writeAll("| FrameParser.parse Benchmarks |\n");
    try writer.writeAll("|------------------------------|\n");
    try bench.run(writer);
}
