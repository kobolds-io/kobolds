const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const benchmark_constants = @import("./constants.zig");
const constants = @import("../constants.zig");
const testing = std.testing;

const Frame = @import("../lib/frame.zig").Frame;
const FrameParser = @import("../lib/frame.zig").FrameParser;
const FrameReassembler = @import("../lib/frame.zig").FrameReassembler;
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
    const small_payload = allocator.alloc(u8, constants.max_frame_payload_size / 10) catch unreachable;
    defer allocator.free(small_payload);

    const medium_payload = allocator.alloc(u8, constants.max_frame_payload_size / 2) catch unreachable;
    defer allocator.free(medium_payload);

    const large_payload = allocator.alloc(u8, constants.max_frame_payload_size) catch unreachable;
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

const ReassembleFromFrames = struct {
    const Self = @This();

    reassembler: *FrameReassembler,
    pool: *MemoryPool(Chunk),
    frames: []Frame,
    pub fn new(reassembler: *FrameReassembler, pool: *MemoryPool(Chunk), frames: []Frame) Self {
        return Self{
            .reassembler = reassembler,
            .pool = pool,
            .frames = frames,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        var payload_length: usize = 0;
        for (self.frames) |frame| {
            payload_length += frame.frame_headers.payload_length;
            const chunk_opt = self.reassembler.reassemble(self.pool, frame) catch unreachable;

            if (chunk_opt) |chunk| {
                var total_chunk_used: usize = 0;
                total_chunk_used += chunk.used;
                var next: ?*Chunk = chunk.next;
                while (next) |next_chunk| {
                    total_chunk_used += next_chunk.used;
                    next = next_chunk.next;
                    self.pool.destroy(next_chunk);
                }
                self.pool.destroy(chunk);

                // std.debug.print("payload len: {}, chunk used: {}\n", .{ payload_length, total_chunk_used });
                // ensure that all the bytes were written to the chunk chain
                assert(payload_length == total_chunk_used);
            }
        }
    }
};

test "FrameAssembler benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var pool = try MemoryPool(Chunk).init(allocator, 1_000);
    defer pool.deinit();

    var assembler = FrameReassembler.new();

    const payload = allocator.alloc(u8, constants.max_frame_payload_size) catch unreachable;
    defer allocator.free(payload);

    // make all the frames
    const single_frame = Frame.new(payload, .{});

    const multi_frame_0 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 0 });
    const multi_frame_1 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 1 });
    const multi_frame_2 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 2 });
    const multi_frame_3 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 3 });
    const multi_frame_4 = Frame.new(payload, .{ .flags = .{ .continuation = false }, .sequence = 4 });

    // TODO: create a benchmark for the assembler.

    var single_frame_frames = [_]Frame{single_frame};
    var multi_frame_frames = [_]Frame{
        multi_frame_0,
        multi_frame_1,
        multi_frame_2,
        multi_frame_3,
        multi_frame_4,
    };

    try bench.addParam(
        "assemble single frame",
        &ReassembleFromFrames.new(&assembler, &pool, &single_frame_frames),
        .{},
    );
    try bench.addParam(
        "assemble multi-frame",
        &ReassembleFromFrames.new(&assembler, &pool, &multi_frame_frames),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|-------------------------------|\n");
    try writer.writeAll("| Assembler.assemble Benchmarks |\n");
    try writer.writeAll("|-------------------------------|\n");
    try bench.run(writer);
}
