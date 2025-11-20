const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const testing = std.testing;

const benchmark_constants = @import("./constants.zig");
const constants = @import("../constants.zig");
const check = @import("../lib/checksum.zig");

const MemoryPool = @import("stdx").MemoryPool;

const Chunk = @import("../lib/chunk.zig").Chunk;
const ChunkReader = @import("../lib/chunk.zig").ChunkReader;
const ChunkWriter = @import("../lib/chunk.zig").ChunkWriter;

const FrameDisassembler = @import("../lib/frame.zig").FrameDisassembler;
const Frame = @import("../lib/frame.zig").Frame;
const FrameParser = @import("../lib/frame.zig").FrameParser;
const FrameReassembler = @import("../lib/frame.zig").FrameReassembler;

const Message = @import("../lib/message.zig").Message;

const SerializeMessage = struct {
    const Self = @This();

    frame_payload_buffer: []u8,
    message: *Message,
    send_buffer: []u8,
    expected_frames_generated_count: usize,

    pub fn new(
        message: *Message,
        frame_payload_buffer: []u8,
        send_buffer: []u8,
        expected_frames_generated_count: usize,
    ) Self {
        return Self{
            .message = message,
            .frame_payload_buffer = frame_payload_buffer,
            .send_buffer = send_buffer,
            .expected_frames_generated_count = expected_frames_generated_count,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        var send_buffer_offset: usize = 0;
        var generated_frames_count: usize = 0;

        var disassembler = FrameDisassembler.new(self.message.chunk, .{});
        while (disassembler.disassemble(self.frame_payload_buffer)) |frame| {
            generated_frames_count += 1;

            if (self.send_buffer[send_buffer_offset..].len < frame.packedSize()) {
                // lets pretend we flushed this send buffer or did something special like caching writing
                // the maximum number of bytes to the send_buffer and caching the remaining bytes
                send_buffer_offset = 0;
            }

            // convert frame to bytes
            const frame_n = frame.toBytes(self.send_buffer[send_buffer_offset..]);
            send_buffer_offset += frame_n;
        }

        assert(self.expected_frames_generated_count == 1);
    }
};

test "Message serialization benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    // the application has created a message and has been tossed around the world
    var tiny_message = try Message.init(&pool, .ping, .{});
    defer tiny_message.deinit(&pool);

    tiny_message.ref();
    defer tiny_message.deref();

    // ensure the message fully overrides the body
    try tiny_message.setBody(&pool, "");

    // the application has created a message and has been tossed around the world
    var small_message = try Message.init(&pool, .ping, .{});
    defer small_message.deinit(&pool);

    small_message.ref();
    defer small_message.deref();

    // ensure the message fully overrides the body
    const small_body = [_]u8{'a'} ** constants.chunk_data_size;
    try small_message.setBody(&pool, &small_body);

    // the application has created a message and has been tossed around the world
    var medium_message = try Message.init(&pool, .ping, .{});
    defer medium_message.deinit(&pool);

    medium_message.ref();
    defer medium_message.deref();

    // ensure the message fully overrides the body
    const medium_body = [_]u8{'a'} ** (constants.chunk_data_size * 10);
    try medium_message.setBody(&pool, &medium_body);

    // the application has created a message and has been tossed around the world
    var large_message = try Message.init(&pool, .ping, .{});
    defer large_message.deinit(&pool);

    large_message.ref();
    defer large_message.deref();

    // ensure the message fully overrides the body
    const large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    try large_message.setBody(&pool, &large_body);

    // create a static frame buffer to be reused
    // FIX: the max_frame_payload_size should be transport dependent
    const frame_payload_buffer = try allocator.alloc(u8, constants.max_frame_payload_size);
    defer allocator.free(frame_payload_buffer);

    // create a static frame buffer to be reused
    // FIX: the send_buffer size should be transport dependent
    const send_buffer = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(send_buffer);

    try bench.addParam(
        "serialize tiny message",
        &SerializeMessage.new(&tiny_message, frame_payload_buffer, send_buffer, 1),
        .{},
    );
    try bench.addParam(
        "serialize small message",
        &SerializeMessage.new(&small_message, frame_payload_buffer, send_buffer, 1),
        .{},
    );
    try bench.addParam(
        "serialize medium message",
        &SerializeMessage.new(&medium_message, frame_payload_buffer, send_buffer, 1),
        .{},
    );
    try bench.addParam(
        "serialize large message",
        &SerializeMessage.new(&large_message, frame_payload_buffer, send_buffer, 1),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|----------------------------------|\n");
    try writer.writeAll("| Message Serialization Benchmarks |\n");
    try writer.writeAll("|----------------------------------|\n");
    try bench.run(writer);
}

const DeserializeMessage = struct {
    const Self = @This();

    pub fn new() Self {
        return Self{};
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        _ = self;

        assert(false);
    }
};

test "Message deserialization benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    // create a message
    // serialize it to bytes
    // write bytes to a `read_buffer`
    // run benchmark that takes bytes off read buffer -> Message

    // try bench.addParam(
    //     "deserialize tiny message",
    //     &DeserializeMessage.new(&tiny_message, frame_payload_buffer, send_buffer, 1),
    //     .{},
    // );
    // try bench.addParam(
    //     "deserialize small message",
    //     &DeserializeMessage.new(&small_message, frame_payload_buffer, send_buffer, 1),
    //     .{},
    // );
    // try bench.addParam(
    //     "deserialize medium message",
    //     &DeserializeMessage.new(&medium_message, frame_payload_buffer, send_buffer, 1),
    //     .{},
    // );
    // try bench.addParam(
    //     "deserialize large message",
    //     &DeserializeMessage.new(&large_message, frame_payload_buffer, send_buffer, 1),
    //     .{},
    // );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|------------------------------------|\n");
    try writer.writeAll("| Message Deserialization Benchmarks |\n");
    try writer.writeAll("|------------------------------------|\n");
    try bench.run(writer);
}
