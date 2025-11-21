const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const benchmark_constants = @import("./constants.zig");
const constants = @import("../constants.zig");
const testing = std.testing;

const Frame = @import("../lib/frame.zig").Frame;
const FrameParser = @import("../lib/frame.zig").FrameParser;
const FrameReassembler = @import("../lib/frame.zig").FrameReassembler;
const FrameDisassembler = @import("../lib/frame.zig").FrameDisassembler;
const MemoryPool = @import("stdx").MemoryPool;
const Chunk = @import("../lib/chunk.zig").Chunk;
const ChunkWriter = @import("../lib/chunk.zig").ChunkWriter;

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

    // this would be a multi send/recv buffer message
    const multi_frame_0 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 0 });
    const multi_frame_1 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 1 });
    const multi_frame_2 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 2 });
    const multi_frame_3 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 3 });
    const multi_frame_4 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 4 });
    const multi_frame_5 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 5 });
    const multi_frame_6 = Frame.new(payload, .{ .flags = .{ .continuation = true }, .sequence = 6 });
    const multi_frame_7 = Frame.new(payload, .{ .flags = .{ .continuation = false }, .sequence = 7 });

    var single_frame_frames = [_]Frame{single_frame};
    var multi_frame_frames = [_]Frame{
        multi_frame_0,
        multi_frame_1,
        multi_frame_2,
        multi_frame_3,
        multi_frame_4,
        multi_frame_5,
        multi_frame_6,
        multi_frame_7,
    };

    try bench.addParam(
        "reassemble single frame",
        &ReassembleFromFrames.new(&assembler, &pool, &single_frame_frames),
        .{},
    );
    try bench.addParam(
        "reassemble multi-frame",
        &ReassembleFromFrames.new(&assembler, &pool, &multi_frame_frames),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|----------------------------------------|\n");
    try writer.writeAll("| FrameReassembler.reassemble Benchmarks |\n");
    try writer.writeAll("|----------------------------------------|\n");
    try bench.run(writer);
}

const DisassembleFramesFromChunks = struct {
    const Self = @This();

    chunk: *Chunk,
    frame_payload_buffer: []u8,
    expected_frames_count: usize,

    pub fn new(chunk: *Chunk, frame_payload_buffer: []u8, expected_frames_count: usize) Self {
        return Self{
            .chunk = chunk,
            .frame_payload_buffer = frame_payload_buffer,
            .expected_frames_count = expected_frames_count,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        var frame_count: usize = 0;

        var disassembler = FrameDisassembler.new(self.chunk, .{});
        while (disassembler.disassemble(self.frame_payload_buffer)) |_| {
            frame_count += 1;
        }

        // std.debug.print("t: {any}\n", .{frame_count});
        // ensure that we have only disassembled the frames we expect
        assert(self.expected_frames_count == frame_count);
    }
};

test "FrameDisassembler benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var pool = try MemoryPool(Chunk).init(allocator, 300);
    defer pool.deinit();

    // make all the chunks
    const single_chunk = try pool.create();
    defer pool.destroy(single_chunk);

    single_chunk.* = Chunk{};
    const single_chunk_payload = try allocator.alloc(u8, constants.chunk_data_size);
    defer allocator.free(single_chunk_payload);

    for (0..single_chunk_payload.len) |i| {
        single_chunk_payload[i] = 99;
    }

    var chunk_writer = ChunkWriter.new(single_chunk);
    try chunk_writer.write(&pool, single_chunk_payload);

    var multi_chunks: std.ArrayList(*Chunk) = .empty;
    defer multi_chunks.deinit(allocator);

    for (0..299) |_| {
        const c = try pool.create();
        c.* = Chunk{};

        try multi_chunks.append(allocator, c);
    }

    var i: usize = 1;
    while (i <= multi_chunks.items.len - 1) : (i += 1) {
        const chunk = multi_chunks.items[i];
        multi_chunks.items[i - 1].next = chunk;
        chunk_writer = ChunkWriter.new(chunk);
        try chunk_writer.write(&pool, single_chunk_payload);
    }

    const frame_payload_buffer = try allocator.alloc(u8, constants.max_frame_payload_size);
    defer allocator.free(frame_payload_buffer);

    try bench.addParam(
        "disassemble single chunk",
        &DisassembleFramesFromChunks.new(single_chunk, frame_payload_buffer, 1),
        .{},
    );
    try bench.addParam(
        "disassemble chunk chain",
        &DisassembleFramesFromChunks.new(multi_chunks.items[0], frame_payload_buffer, 19),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|------------------------------------------|\n");
    try writer.writeAll("| FrameDisassembler.disassemble Benchmarks |\n");
    try writer.writeAll("|------------------------------------------|\n");
    try bench.run(writer);
}
