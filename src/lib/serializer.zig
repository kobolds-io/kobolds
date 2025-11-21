const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const FrameDisassembler = @import("./frame.zig").FrameDisassembler;
const ChunkReader = @import("./chunk.zig").ChunkReader;
const Chunk = @import("./chunk.zig").Chunk;
const Frame = @import("./frame.zig").Frame;
const FixedHeaders = @import("./message.zig").FixedHeaders;
const ExtensionHeaders = @import("./message.zig").ExtensionHeaders;
const Message = @import("./message.zig").Message;

const MemoryPool = @import("stdx").MemoryPool;

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

pub const Serializer2 = struct {
    // have an overflow buffer

};

pub const Serializer = struct {
    const Self = @This();

    /// Temporary buffer for holding a frame payload before encoding it into a Frame.
    frame_payload_buffer: std.ArrayList(u8),
    current_chunk: ?*Chunk = null,

    pub fn initCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
        return Self{
            .frame_payload_buffer = try std.ArrayList(u8).initCapacity(allocator, capacity),
            .current_chunk = null,
        };
    }

    pub const empty = Self{
        .frame_payload_buffer = .empty,
        .current_chunk = null,
    };

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.frame_payload_buffer.deinit(allocator);
    }

    /// Serializes an entire message into the buffer `out`.
    /// Returns: number of bytes written.
    pub fn serialize(
        self: *Self,
        allocator: std.mem.Allocator,
        message: *Message,
        out: []u8,
    ) !usize {
        if (self.frame_payload_buffer.items.len < constants.max_frame_payload_size) {
            try self.frame_payload_buffer.resize(allocator, constants.max_frame_payload_size);
        }

        var dis = FrameDisassembler.new(
            message.chunk,
            .{ .max_frame_payload_size = self.frame_payload_buffer.capacity },
        );

        var write_index: usize = 0;

        while (true) {
            // // Reset payload buffer to empty for the next frame.
            // self.frame_payload_buffer.shrinkRetainingCapacity(0);

            // Ask the disassembler for the next frame.
            const next_frame_opt = dis.disassemble(self.frame_payload_buffer.items);
            if (next_frame_opt == null) break; // no more frames

            var frame = next_frame_opt.?;

            // Determine required size
            const frame_size = frame.packedSize();
            if (write_index + frame_size > out[write_index..].len) return error.OutputBufferTooSmall;

            // Write the frame into the output buffer
            const frame_n = frame.toBytes(out[write_index..]);

            // ensure that the frame isn't broken
            assert(frame_size == frame_n);

            write_index += frame_n;
        }

        return write_index;
    }
};

// NOTE: This is kind of complex and doesn't work like I want it to
// pub const Serializer = struct {
//     const Self = @This();

//     /// scratch space used to hold header bytes when we need to write them partially.
//     /// sized large enough for all headers; adjust if your headers can be bigger.
//     header_scratch: [512]u8,

//     /// If non-null, a message is currently being serialized (we haven't finished).
//     current_message: ?*Message,

//     /// offsets inside header_scratch for partially-written headers
//     fixed_hdr_total: usize,
//     fixed_hdr_written: usize,

//     ext_hdr_total: usize,
//     ext_hdr_written: usize,

//     /// body streaming state
//     body_reader: ?ChunkReader,
//     body_remaining: usize,

//     frame_payload_buffer: std.ArrayList(u8), // kept for compatibility with earlier struct shape

//     pub fn initCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
//         return Self{
//             .header_scratch = undefined,
//             .current_message = null,
//             .fixed_hdr_total = 0,
//             .fixed_hdr_written = 0,
//             .ext_hdr_total = 0,
//             .ext_hdr_written = 0,
//             .body_reader = null,
//             .body_remaining = 0,
//             .frame_payload_buffer = try std.ArrayList(u8).initCapacity(allocator, capacity),
//         };
//     }

//     pub const empty = Self{
//         .header_scratch = undefined,
//         .current_message = null,
//         .fixed_hdr_total = 0,
//         .fixed_hdr_written = 0,
//         .ext_hdr_total = 0,
//         .ext_hdr_written = 0,
//         .body_reader = null,
//         .body_remaining = 0,
//         .frame_payload_buffer = .empty,
//     };

//     pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
//         self.frame_payload_buffer.deinit(allocator);
//     }

//     /// Begin (or continue) serialization of `message` into `out`.
//     /// On the first call for a given message, the serializer snapshots the headers
//     /// into an internal scratch buffer so that partial writes of headers are easy.
//     ///
//     /// Usage pattern:
//     ///  - call serialize(message, out) repeatedly until it returns n == total_message_bytes
//     ///  - if the serializer is mid-message, you must call with the same message pointer
//     ///
//     /// Returns: number of bytes written to `out` for this call.
//     pub fn serialize(self: *Self, allocator: std.mem.Allocator, message: *Message, out: []u8) !usize {
//         _ = allocator;
//         // quick bail if nothing to write
//         if (out.len == 0) return 0;

//         // If we're already serializing a different message, that's an error.
//         if (self.current_message) |cur| {
//             if (cur != message) return error.AlreadySerializing;
//         } else {
//             // Initialize serialization state for this message.
//             self.current_message = message;

//             // --- Fixed headers into scratch ---
//             const fixed_size = FixedHeaders.packedSize();
//             if (fixed_size > self.header_scratch.len) return error.ScratchTooSmall;

//             // Write fixed headers into scratch buffer using message.fixed_headers.toBytes
//             _ = message.fixed_headers.toBytes(self.header_scratch[0..fixed_size]);
//             self.fixed_hdr_total = fixed_size;
//             self.fixed_hdr_written = 0;

//             // --- Extension headers into scratch (variable-size) ---
//             // We try to determine ext header size via a helper.
//             const ext_size = ExtensionHeaders.packedSize(message.fixed_headers.message_type);
//             if (fixed_size + ext_size > self.header_scratch.len) return error.ScratchTooSmall;

//             _ = message.extension_headers.toBytes(self.header_scratch[fixed_size .. fixed_size + ext_size]);
//             self.ext_hdr_total = ext_size;
//             self.ext_hdr_written = 0;

//             // --- Body reader initialization ---
//             // Expected message API: message.body_len() and message.body_start_chunk() & offset
//             self.body_remaining = message.bodySize();
//             // const start_chunk = message.body_start_chunk();
//             // const start_offset = message.headersPackedSize();

//             self.body_reader = message.bodyReader();
//             // // advance the reader to start_offset
//             // if (start_offset > 0) {
//             //     // we can skip start_offset bytes by reading into a small temp
//             //     var tmp_buf: [64]u8 = undefined;
//             //     var to_skip = start_offset;
//             //     while (to_skip > 0) {
//             //         const take = @min(to_skip, tmp_buf.len);
//             //         const r = self.body_reader.?.read(tmp_buf[0..take]);
//             //         if (r == 0) return error.UnexpectedEOF;
//             //         to_skip -= r;
//             //     }
//             // }
//         }

//         // Now stream as many bytes as will fit in `out`, across:
//         // fixed headers (remaining), ext headers (remaining), body (remaining).
//         var out_index: usize = 0;

//         // 1) write remainder of fixed headers from scratch
//         while (self.fixed_hdr_written < self.fixed_hdr_total and out_index < out.len) {
//             const src_slice = self.header_scratch[self.fixed_hdr_written..self.fixed_hdr_total];
//             const to_copy = @min(src_slice.len, out.len - out_index);
//             @memcpy(out[out_index .. out_index + to_copy], src_slice[0..to_copy]);
//             out_index += to_copy;
//             self.fixed_hdr_written += to_copy;
//         }

//         // If we finished fixed headers, proceed to ext headers
//         while (self.fixed_hdr_written == self.fixed_hdr_total and
//             self.ext_hdr_written < self.ext_hdr_total and
//             out_index < out.len)
//         {
//             const src_slice = self.header_scratch[self.fixed_hdr_total + self.ext_hdr_written .. self.fixed_hdr_total + self.ext_hdr_total];
//             const to_copy = @min(src_slice.len, out.len - out_index);
//             @memcpy(out[out_index .. out_index + to_copy], src_slice[0..to_copy]);
//             out_index += to_copy;
//             self.ext_hdr_written += to_copy;
//         }

//         // 3) write body bytes via ChunkReader
//         if (out_index < out.len and self.body_remaining > 0) {
//             // write into the remainder of out
//             const write_buf = out[out_index..];
//             const r = self.body_reader.?.read(write_buf);
//             out_index += r;
//             if (r > 0) {
//                 if (r > self.body_remaining) {
//                     // should not happen; defensive clamp
//                     self.body_remaining = 0;
//                 } else {
//                     self.body_remaining -= r;
//                 }
//             }
//         }

//         // If we've consumed everything, reset state
//         if (self.fixed_hdr_written == self.fixed_hdr_total and
//             self.ext_hdr_written == self.ext_hdr_total and
//             self.body_remaining == 0)
//         {

//             // done with message
//             self.current_message = null;
//             self.body_reader = null;
//             self.fixed_hdr_total = 0;
//             self.fixed_hdr_written = 0;
//             self.ext_hdr_total = 0;
//             self.ext_hdr_written = 0;
//             self.body_remaining = 0;
//         }

//         return out_index;
//     }
// };

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

    const bytes_written = try serializer.serialize(allocator, &message, out_buffer);

    // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});

    // (size of the message and size of the frame)
    const expected_size = message.packedSize() + 12;

    try testing.expectEqual(expected_size, bytes_written);
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

    // try testing.expect(message.bodySize() > out_buffer.len);

    const bytes_written = try serializer.serialize(allocator, &message, out_buffer);

    // std.debug.print("out_buffer: {any}\n", .{out_buffer[0..bytes_written]});

    try testing.expectEqual(81954, bytes_written);
}
