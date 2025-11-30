const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const FrameReassembler = @import("./frame.zig").FrameReassembler;
const FrameDisassembler = @import("./frame.zig").FrameDisassembler;
const Frame = @import("./frame.zig").Frame;
const FrameParser = @import("./frame.zig").FrameParser;

const ChunkReader = @import("./chunk.zig").ChunkReader;
const Chunk = @import("./chunk.zig").Chunk;

const FixedHeaders = @import("./message.zig").FixedHeaders;
const ExtensionHeaders = @import("./message.zig").ExtensionHeaders;
const Message = @import("./message.zig").Message;

const MemoryPool = @import("stdx").MemoryPool;
const RingBuffer = @import("stdx").RingBuffer;

pub const Serializer = struct {
    const Self = @This();

    const State = enum {
        ready,
        partial,
        noop,
    };

    const SerializeResult = struct {
        total_bytes_written: usize,
        message_bytes_remaining: usize,
        frames_written: usize,
        state: State,
    };

    /// Temporary buffer for holding a frame payload before encoding it into a Frame.
    frame_payload_buffer: []u8,
    state: State,
    message: ?*Message,
    message_chunk_offset: usize,
    message_bytes_remaining: usize,

    pub fn initCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
        const frame_payload_buffer = try allocator.alloc(u8, capacity);
        errdefer allocator.free(frame_payload_buffer);

        return Self{
            .frame_payload_buffer = frame_payload_buffer,
            .state = .ready,
            .message = null,
            .message_chunk_offset = 0,
            .message_bytes_remaining = 0,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.frame_payload_buffer);
    }

    pub fn feed(self: *Self, message: *Message) void {
        assert(self.state == .ready);
        assert(self.message == null);

        self.message = message;
        self.message_chunk_offset = 0;
        self.message_bytes_remaining = message.packedSize();
    }

    pub fn serialize(self: *Self, out: []u8) !SerializeResult {
        // No message -> nothing to do
        if (self.message == null) {
            self.state = .ready;
            return SerializeResult{
                .total_bytes_written = 0,
                .message_bytes_remaining = 0,
                .frames_written = 0,
                .state = .noop,
            };
        }

        if (out.len <= Frame.minimumSize()) return error.OutputBufferTooSmall;

        var total_written: usize = 0;
        var frames_written: usize = 0;

        const message = self.message.?;

        // find the correct chunk + index for the current offset
        var chunk: ?*Chunk = message.chunk;
        var chunk_offset_acc: usize = 0;

        while (chunk) |c| {
            // correct chunk has been found
            if (self.message_chunk_offset < chunk_offset_acc + c.used) break;
            chunk_offset_acc += c.used;
            chunk = c.next;
        }

        // something we didn't expect happened.
        assert(chunk != null);

        var current_chunk = chunk.?;
        var chunk_local_index = self.message_chunk_offset - chunk_offset_acc;

        // assemble frames until out_buffer cannot fit more
        while (true) {
            const remaining = out.len - total_written;
            if (remaining <= Frame.minimumSize())
                break;

            const max_payload_fit = remaining - Frame.minimumSize();
            const payload_cap = @min(self.frame_payload_buffer.len, max_payload_fit);

            // Fresh disassembler at the correct chunk & index
            var disassembler = FrameDisassembler.new(current_chunk, .{
                .max_frame_payload_size = self.frame_payload_buffer.len,
                .offset = chunk_local_index,
            });

            // next frame
            const frame_opt = disassembler.disassemble(self.frame_payload_buffer[0..payload_cap]);
            if (frame_opt == null) break;

            const frame = frame_opt.?;

            const frame_size = frame.packedSize();

            // we check this earlier so this should never happen unless there is a logical error
            assert(frame_size <= remaining);

            // write frame bytes
            const n = frame.toBytes(out[total_written..]);
            total_written += n;
            frames_written += 1;

            // advance message state
            var payload_used = n - Frame.minimumSize();
            self.message_chunk_offset += payload_used;
            self.message_bytes_remaining -= payload_used;

            // move into next chunk if needed
            while (chunk_local_index + payload_used >= current_chunk.used) {
                payload_used -= (current_chunk.used - chunk_local_index);
                if (current_chunk.next == null) break;
                current_chunk = current_chunk.next.?;
                chunk_local_index = 0;
            } else {
                chunk_local_index += payload_used;
            }

            if (self.message_bytes_remaining == 0) break;
        }

        // full write
        if (self.message_bytes_remaining == 0) {
            self.message = null;
            self.message_chunk_offset = 0;
            self.state = .ready;

            return SerializeResult{
                .total_bytes_written = total_written,
                .message_bytes_remaining = 0,
                .frames_written = frames_written,
                .state = .ready,
            };
        }

        // partial write
        self.state = .partial;
        return SerializeResult{
            .total_bytes_written = total_written,
            .message_bytes_remaining = self.message_bytes_remaining,
            .frames_written = frames_written,
            .state = .partial,
        };
    }
};

pub const Deserializer = struct {
    const Self = @This();

    const State = enum {
        ready,
        partial,
        noop,
    };

    const SerializeResult = struct {
        total_bytes_written: usize,
        message_bytes_remaining: usize,
        frames_written: usize,
        state: State,
    };

    frames_buffer: []Frame,
    frames_parser: FrameParser,

    pub fn initCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
        const frames_buffer = try allocator.alloc(Frame, capacity);
        errdefer allocator.free(frames_buffer);

        return Self{
            .frames_buffer = frames_buffer,
            .frames_parser = FrameParser.init(allocator),
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.frames_buffer);
        self.frames_parser.deinit(allocator);
    }

    pub fn deserialize(
        self: *Self,
        allocator: std.mem.Allocator,
        pool: *MemoryPool(Chunk),
        in: []const u8,
    ) !void {
        const parse_result = try self.frames_parser.parse(self.frames_buffer, in);

        // if there are no bytes left in the `in`

        std.debug.print("parse_result: {any}\n", .{parse_result});

        _ = allocator;
        _ = pool;
    }
};

test "serializer writes partial message to buffer" {
    const allocator = testing.allocator;

    var serializer = try Serializer.initCapacity(allocator, 1024);
    defer serializer.deinit(allocator);

    var pool = try MemoryPool(Chunk).init(allocator, 500);
    defer pool.deinit();

    const out_buffer = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(out_buffer);

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 100);
    large_body[large_body.len - 1] = 'b';
    try message.setBody(&pool, &large_body);

    try testing.expectEqual(.ready, serializer.state);

    // feed this message to the serializer
    serializer.feed(&message);

    var total_bytes_written: usize = 0;
    var total_frames_written: usize = 0;
    while (true) {
        var bytes_written: usize = 0;
        const result = try serializer.serialize(out_buffer[bytes_written..]);
        total_bytes_written += result.total_bytes_written;
        total_frames_written += result.frames_written;
        // defer {
        //     std.debug.print("result: {any}\n", .{result});
        // }
        switch (result.state) {
            .ready => break,
            .partial => {
                bytes_written += result.total_bytes_written;
                if (result.message_bytes_remaining > out_buffer[bytes_written..].len) {
                    // FLUSH THE BUFFER!
                    // std.debug.print("flushing the buffer!\n", .{});
                    bytes_written = 0;
                }
            },
            else => assert(false), // this should never happen unless we've configured incorrectly
        }
    }

    try testing.expectEqual(.ready, serializer.state);
    try testing.expectEqual(
        total_bytes_written,
        message.packedSize() + (total_frames_written * Frame.minimumSize()),
    );
}

test "serialize many messages" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 500);
    defer pool.deinit();

    const out_buffer = try allocator.alloc(u8, 1024 * 64);
    defer allocator.free(out_buffer);

    const messages = try allocator.alloc(Message, 10);
    defer allocator.free(messages);

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    large_body[large_body.len - 1] = 'b';
    for (0..messages.len) |i| {
        var message = try Message.init(&pool, .ping, .{});
        errdefer message.deinit(&pool);

        try message.setBody(&pool, &large_body);
        messages[i] = message;
    }

    var serializer = try Serializer.initCapacity(allocator, 500);
    defer serializer.deinit(allocator);

    const total_message_bytes = messages[0].packedSize() * messages.len;

    var total_bytes_written: usize = 0;
    var total_frames_written: usize = 0;
    for (0..messages.len) |i| {
        var message = messages[i];

        if (serializer.state == .ready) serializer.feed(&message);

        while (true) {
            var bytes_written: usize = 0;
            const result = try serializer.serialize(out_buffer[bytes_written..]);
            total_bytes_written += result.total_bytes_written;
            total_frames_written += result.frames_written;
            // defer {
            //     std.debug.print("result: {any}\n", .{result});
            // }
            switch (result.state) {
                .ready => break,
                .partial => {
                    bytes_written += result.total_bytes_written;
                    if (result.message_bytes_remaining > out_buffer[bytes_written..].len) {
                        // FLUSH THE BUFFER!
                        // std.debug.print("flushing the buffer!\n", .{});
                        bytes_written = 0;
                    }
                },
                else => assert(false), // this should never happen unless we've configured incorrectly
            }
        }
    }

    try testing.expectEqual(.ready, serializer.state);
    try testing.expectEqual(
        total_bytes_written,
        total_message_bytes + (total_frames_written * Frame.minimumSize()),
    );
}

test "deserialize takes bytes and makes a message" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    try message.setBody(&pool, "test body");

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    large_body[large_body.len - 1] = 'b';
    try message.setBody(&pool, &large_body);

    var serializer = try Serializer.initCapacity(allocator, constants.max_frame_payload_size);
    defer serializer.deinit(allocator);

    // try testing.expect(message.bodySize() > out_buffer.len);

    // const serialize_result = try serializer.serialize2(allocator, &message);

    // std.debug.print("frames: {any}\n", .{serializer.frames.items.len});
    // std.debug.print("serialize_result: {any}\n", .{serialize_result});
    // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});

    // convert the message to bytes

}
