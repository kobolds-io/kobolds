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

const Serializer = @import("../lib/serde.zig").Serializer;

const FrameDisassembler = @import("../lib/frame.zig").FrameDisassembler;
const Frame = @import("../lib/frame.zig").Frame;
const FrameParser = @import("../lib/frame.zig").FrameParser;
const FrameReassembler = @import("../lib/frame.zig").FrameReassembler;

const Message = @import("../lib/message.zig").Message;

const SerializeMessages = struct {
    const Self = @This();

    serializer: *Serializer,
    messages: []Message,
    out: []u8,

    pub fn new(serializer: *Serializer, messages: []Message, out: []u8) Self {
        return Self{
            .messages = messages,
            .serializer = serializer,
            .out = out,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        for (0..self.messages.len) |i| {
            assert(self.serializer.state == .ready);

            var message = self.messages[i];
            self.serializer.feed(&message);

            while (true) {
                var bytes_written: usize = 0;
                const result = self.serializer.serialize(self.out[bytes_written..]) catch unreachable;
                switch (result.state) {
                    .ready => break,
                    .partial => {
                        bytes_written += result.total_bytes_written;
                        if (result.message_bytes_remaining > self.out[bytes_written..].len) {
                            // FLUSH THE BUFFER!
                            // std.debug.print("flushing the buffer!\n", .{});
                            bytes_written = 0;
                        }
                    },
                    else => assert(false), // this should never happen unless we've configured incorrectly
                }
            }
        }
    }
};

test "serializer benchmarks" {
    const allocator = testing.allocator;
    var bench = zbench.Benchmark.init(allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var pool = try MemoryPool(Chunk).init(allocator, 1_000);
    defer pool.deinit();

    const out = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(out);

    const small_messages = try allocator.alloc(Message, 10);
    defer allocator.free(small_messages);

    // ensure the message fully overrides the body
    var small_body = [_]u8{'a'} ** (constants.chunk_data_size);
    small_body[small_body.len - 1] = 'b';
    for (0..small_messages.len) |i| {
        var message = try Message.init(&pool, .ping, .{});
        errdefer message.deinit(&pool);

        try message.setBody(&pool, &small_body);
        small_messages[i] = message;
    }

    const medium_messages = try allocator.alloc(Message, 10);
    defer allocator.free(medium_messages);

    // ensure the message fully overrides the body
    var medium_body = [_]u8{'a'} ** (constants.chunk_data_size * 10);
    medium_body[medium_body.len - 1] = 'b';
    for (0..medium_messages.len) |i| {
        var message = try Message.init(&pool, .ping, .{});
        errdefer message.deinit(&pool);

        try message.setBody(&pool, &medium_body);
        medium_messages[i] = message;
    }

    const large_messages = try allocator.alloc(Message, 10);
    defer allocator.free(large_messages);

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    large_body[large_body.len - 1] = 'b';
    for (0..large_messages.len) |i| {
        var message = try Message.init(&pool, .ping, .{});
        errdefer message.deinit(&pool);

        try message.setBody(&pool, &large_body);
        large_messages[i] = message;
    }

    var serializer = try Serializer.initCapacity(allocator, constants.max_frame_payload_size);
    defer serializer.deinit(allocator);

    // std.debug.print("small: {}, medium: {}, large: {}\n", .{ small_body.len, medium_body.len, large_body.len });

    try bench.addParam(
        "serialize small messages",
        &SerializeMessages.new(&serializer, small_messages, out),
        .{},
    );
    try bench.addParam(
        "serialize medium messages",
        &SerializeMessages.new(&serializer, medium_messages, out),
        .{},
    );
    try bench.addParam(
        "serialize large messages",
        &SerializeMessages.new(&serializer, large_messages, out),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|-----------------------|\n");
    try writer.writeAll("| Serializer Benchmarks |\n");
    try writer.writeAll("|-----------------------|\n");
    try bench.run(writer);
}
