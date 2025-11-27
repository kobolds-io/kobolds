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

// we can imagine that the serializer will be fed a series of messages
// 1. serializer.serialize(msg_1)
// 1. serializer.serialize(msg_2)
// 1. serializer.serialize(msg_3)
//
// I think that we can have the serializer act like a state machine

// var serializer = Serializer.init();
//
//  in some loop ...
//
// switch(serializer.state) {
//  .ready => {
//      // out_buffer is not full and can accept another message
//      // if there is another message, then try to serialize it
//      if (next_message) {
//         out_buffer_offset = serializer.serialize(next_message, out_buffer[out_buffer_offset..]);
//      } else {
//         // flush the out_buffer
//      }
//   },
//   .out_buffer_full => {
//     // flush the out_buffer
//     flusher(out_buffer[..out_buffer_offset]);
//
//     // assume the out_buffer is empty now
//     out_buffer_offset = 0;
//
//     // reset the serializer state
//
//   },
//   .err => {
//      // somthing bad happened
//   }
//
// }

pub const Deserializer = struct {
    const Self = @This();

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

pub const Serializer = struct {
    const Self = @This();

    const State = enum {
        ready,
        partial,
        noop,
    };

    const SerializeResult = struct {
        bytes_written: usize,
        bytes_remaining: usize,
        state: State,
    };

    /// Temporary buffer for holding a frame payload before encoding it into a Frame.
    frame_payload_buffer: []u8,
    state: State,
    message: ?*Message,
    offset: usize,

    pub fn initCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
        const frame_payload_buffer = try allocator.alloc(u8, capacity);
        errdefer allocator.free(frame_payload_buffer);

        return Self{
            .frame_payload_buffer = frame_payload_buffer,
            .state = .ready,
            .message = null,
            .offset = 0,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.frame_payload_buffer);
    }

    pub fn feed(self: *Self, message: *Message) void {
        assert(self.state == .ready);
        assert(self.message == null);

        self.message = message;
        self.offset = 0;
    }

    pub fn serialize2(self: *Self, out_buffer: []u8) !SerializeResult {
        if (self.message == null or out_buffer.len == 0) return SerializeResult{
            .bytes_written = 0,
            .bytes_remaining = 0,
            .state = .noop,
        };

        return SerializeResult{
            .bytes_written = 0,
            .bytes_remaining = 0,
            .state = .noop,
        };

        // const message_size = message.packedSize();
        // const frames_required = message_size / self.frame_payload_buffer.len;
        // const total_bytes_required = message_size + (Frame.minimumSize() * frames_required);

        // var bytes_written: usize = 0;

        // var disassembler = FrameDisassembler.new(message.chunk, .{
        //     .max_frame_payload_size = self.frame_payload_buffer.len,
        // });

        // if (total_bytes_required <= out_buffer.len) {
        //     // convert the entire message -> []Frames -> []const u8
        //     while (disassembler.disassemble(self.frame_payload_buffer)) |frame| {
        //         const frame_n = frame.toBytes(out_buffer[bytes_written..]);

        //         bytes_written += frame_n;
        //     }

        //     // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});
        //     return SerializeResult{
        //         .bytes_written = bytes_written,
        //         .bytes_remaining = 0,
        //         .state = .ready,
        //     };
        // } else {
        //     // there is going to be some sort of overflow, figure out the maxiumum number of bytes that can be written.

        //     return SerializeResult{
        //         .bytes_written = 0,
        //         .bytes_remaining = total_bytes_required,
        //         .state = .partial,
        //     };
        // }

        // std.debug.print("total_bytes_required: {}\n", .{total_bytes_required});

        // TODO:
        //   1. figure out how many bytes are available in the out_buffer
        //   2. figure out how large this message is
        //   3. if the frames that make up this message, figure out how many bytes can be written at a time
        //   4.

        // // handle the overflow

        // while (self.messages.dequeue()) |message| {
        //     // convert this message to bytes
        // }
    }

    pub fn serialize(self: *Self, message: *Message, out_buffer: []u8) !SerializeResult {
        // there is no work for us to do
        if (out_buffer.len == 0) return SerializeResult{
            .bytes_written = 0,
            .bytes_remaining = 0,
            .state = .noop,
        };

        const message_size = message.packedSize();
        const frames_required = message_size / self.frame_payload_buffer.len;
        const total_bytes_required = message_size + (Frame.minimumSize() * frames_required);

        var bytes_written: usize = 0;

        var disassembler = FrameDisassembler.new(message.chunk, .{
            .max_frame_payload_size = self.frame_payload_buffer.len,
        });

        if (total_bytes_required <= out_buffer.len) {
            // convert the entire message -> []Frames -> []const u8
            while (disassembler.disassemble(self.frame_payload_buffer)) |frame| {
                const frame_n = frame.toBytes(out_buffer[bytes_written..]);

                bytes_written += frame_n;
            }

            // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});
            return SerializeResult{
                .bytes_written = bytes_written,
                .bytes_remaining = 0,
                .state = .ready,
            };
        } else {
            // there is going to be some sort of overflow, figure out the maxiumum number of bytes that can be written.

            return SerializeResult{
                .bytes_written = 0,
                .bytes_remaining = total_bytes_required,
                .state = .partial,
            };
        }

        // std.debug.print("total_bytes_required: {}\n", .{total_bytes_required});

        // TODO:
        //   1. figure out how many bytes are available in the out_buffer
        //   2. figure out how large this message is
        //   3. if the frames that make up this message, figure out how many bytes can be written at a time
        //   4.

        // // handle the overflow

        // while (self.messages.dequeue()) |message| {
        //     // convert this message to bytes
        // }
    }
};

test "serializer writes message to buffer" {
    const allocator = testing.allocator;

    var serializer = try Serializer.initCapacity(allocator, constants.max_frame_payload_size);
    defer serializer.deinit(allocator);

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    const out_buffer = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(out_buffer);

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    // try serializer.feed(&message);

    // const serialize_result = try serializer.serialize(allocator, &message, out_buffer);

    // (size of the message and size of the frame)
    // const expected_size = message.packedSize() + 12;

    // try testing.expectEqual(expected_size, serialize_result.bytes_written);
}

test "serializer writes partial message to buffer" {
    const allocator = testing.allocator;

    var serializer = try Serializer.initCapacity(allocator, constants.max_frame_payload_size);
    defer serializer.deinit(allocator);

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    const out_buffer = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(out_buffer);

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    large_body[large_body.len - 1] = 'b';
    try message.setBody(&pool, &large_body);

    try testing.expectEqual(.ready, serializer.state);

    serializer.feed(&message);

    const result = try serializer.serialize2(out_buffer);

    // try testing.expect(message.bodySize() > out_buffer.len);

    // const result = try serializer.serialize(&message, out_buffer);
    std.debug.print("result: {any}\n", .{result});

    // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});

    // try testing.expect(serialize_result.bytes_written <= out_buffer.len);
    // try testing.expectEqual(81954, serialize_result.bytes_written);
}

test "serialize many messages" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 100);
    defer pool.deinit();

    const out_buffer = try allocator.alloc(u8, 1024 * 256);
    defer allocator.free(out_buffer);

    const messages = try allocator.alloc(Message, 10);
    defer allocator.free(messages);

    // ensure the message fully overrides the body
    var large_body = [_]u8{'a'} ** (constants.chunk_data_size * 20);
    large_body[large_body.len - 1] = 'b';
    for (0..messages.len) |i| {
        var message = try Message.init(&pool, .ping, .{});
        defer message.deinit(&pool);

        try message.setBody(&pool, &large_body);
        messages[i] = message;
    }

    var serializer = try Serializer.initCapacity(allocator, constants.max_frame_payload_size);
    defer serializer.deinit(allocator);

    // take a message serialize it into bytes.
    // Message -> []Frame -> []const u8

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
