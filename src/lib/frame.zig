const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const check = @import("./checksum.zig");
const constants = @import("../constants.zig");

const ProtocolVersion = @import("protocol.zig").ProtocolVersion;

pub const Frame = struct {
    const Self = @This();

    pub const Options = struct {
        flags: FrameHeadersFlags = .{},
        sequence: u16 = 0,
    };

    /// Metadata about the frame
    frame_headers: FrameHeaders = .{},

    /// Body of the frame. May contain a `Message` or a segment of a message.
    payload: []const u8,

    /// Checksum of the frame header + payload
    checksum: u32 = 0,

    pub fn packedSize(self: Self) usize {
        return FrameHeaders.packedSize() + self.payload.len + @sizeOf(u32);
    }

    pub fn new(payload: []const u8, opts: Options) Self {
        // NOTE: for now, we just discard the options As features are implemented for each flag,
        // they will be handled.

        assert(payload.len <= std.math.maxInt(u16));

        return Self{
            .frame_headers = FrameHeaders{
                .payload_length = @intCast(payload.len),
                .flags = opts.flags,
                .sequence = opts.sequence,
            },

            .payload = payload,
            .checksum = 0,
        };
    }

    /// Converts the struct into bytes and orders them. Endieness is Big.
    /// Order is as follows: [ frame_header | payload | checksum ]
    pub fn toBytes(self: *Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());

        var write_offset: usize = 0;
        const frame_header_bytes = self.frame_headers.toBytes(buf);
        write_offset += frame_header_bytes;

        @memcpy(buf[write_offset .. write_offset + self.payload.len], self.payload);
        write_offset += self.payload.len;

        // calculate a checksum for all the bytes in the buffer BEFORE the checksum
        const checksum = check.xxHash32Checksum(buf[0..write_offset]);
        self.checksum = checksum;

        std.mem.writeInt(u32, buf[write_offset..][0..@sizeOf(u32)], self.checksum, .big);
        write_offset += @sizeOf(u32);

        return write_offset;
    }

    /// Reads bytes from a buffer and casts those bytes into a Frame. The order must be the same
    /// order of the `Frame.toBytes` function.
    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < FrameHeaders.packedSize()) return error.Truncated;

        var read_offset: usize = 0;

        const frame_headers = try FrameHeaders.fromBytes(data[read_offset..]);
        read_offset += FrameHeaders.packedSize();

        const payload_len = frame_headers.payload_length;
        if (data.len < read_offset + payload_len) return error.Truncated;

        // reference the slice of the data as not copy
        const payload = data[read_offset .. read_offset + payload_len];
        read_offset += payload_len;

        const checksum = std.mem.readInt(u32, data[read_offset..][0..@sizeOf(u32)], .big);

        // recalculate the checksum to verify the integrity of the frame. This checksum calculation
        // only includes the frame_headers and the payload, not the checksum itself.
        if (!check.xxHash32Verify(checksum, data[0..read_offset])) return error.InvalidChecksum;

        read_offset += @sizeOf(u32);

        return Self{
            .frame_headers = frame_headers,
            .payload = payload,
            .checksum = checksum,
        };
    }
};

pub const FrameHeaders = struct {
    const Self = @This();

    comptime {
        // if this ever changes we will explode during compile time
        assert(8 == FrameHeaders.packedSize());
    }

    /// A magical resync value "KB" (kobold)
    magic: u16 = constants.frame_headers_magic,

    /// The protocol version this frame complies with
    protocol_version: ProtocolVersion = .v1,

    /// The purpose of this frame
    frame_type: FrameType = .message,

    /// Bitmask containing flags describing this frame and its contents
    flags: FrameHeadersFlags = .{},

    /// sequence number of the is frame.
    sequence: u16 = 0,

    /// length of the payload in bytes
    payload_length: u16 = 0,

    /// Returns the number of bytes this struct consumes once serialized to bytes
    pub fn packedSize() usize {
        return @sizeOf(u16) +
            1 + // protocol_version and flags combine to a single byte
            @sizeOf(FrameHeadersFlags) +
            @sizeOf(u16) +
            @sizeOf(u16);
    }

    /// Write the bytes that make up the FrameHeader to the buffer.
    /// [ magic | protocol_version + frame_type | flags | sequence | payload_length ]
    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= FrameHeaders.packedSize());

        var write_offset: usize = 0;

        std.mem.writeInt(u16, buf[write_offset..][0..@sizeOf(u16)], self.magic, .big);
        write_offset += @sizeOf(u16);

        // both protocol_version and frame_type are u4s so we combine them into a u8
        buf[write_offset] = (@as(u8, @intFromEnum(self.protocol_version)) << 4) | @as(u8, @intFromEnum(self.frame_type));
        write_offset += 1;

        buf[write_offset] = @as(u8, @bitCast(self.flags));
        write_offset += 1;

        std.mem.writeInt(u16, buf[write_offset..][0..@sizeOf(u16)], self.sequence, .big);
        write_offset += @sizeOf(u16);

        std.mem.writeInt(u16, buf[write_offset..][0..@sizeOf(u16)], self.payload_length, .big);
        write_offset += @sizeOf(u16);

        return write_offset;
    }

    /// Read the bytes that make up a FrameHeader from the buffer.
    /// [magic, protocol_version + frame_type, flags, sequence, payload_length, checksum]
    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.packedSize()) return error.Truncated;
        var read_offset: usize = 0;

        const magic = std.mem.readInt(u16, data[read_offset..][0..@sizeOf(u16)], .big);
        read_offset += @sizeOf(u16);

        // if the magic bytes do not equal what we think they should equal then this is not a valid
        // frame and therefore we should fail
        if (magic != constants.frame_headers_magic) return error.InvalidFrameHeadersMagic;

        const protocol_version_and_frame_type = data[read_offset];
        const protocol_version_bits: u4 = @intCast(protocol_version_and_frame_type >> 4);
        const protocol_version = ProtocolVersion.fromBits(protocol_version_bits);

        const frame_type_bits: u4 = @intCast(protocol_version_and_frame_type & 0xF);
        const frame_type = try FrameType.fromBits(frame_type_bits);

        read_offset += 1;

        const flags: FrameHeadersFlags = @bitCast(data[read_offset]);
        read_offset += 1;

        const sequence = std.mem.readInt(u16, data[read_offset..][0..@sizeOf(u16)], .big);
        read_offset += @sizeOf(u16);

        const payload_length = std.mem.readInt(u16, data[read_offset..][0..@sizeOf(u16)], .big);
        read_offset += @sizeOf(u16);

        return FrameHeaders{
            .magic = magic,
            .protocol_version = protocol_version,
            .frame_type = frame_type,
            .flags = flags,
            .sequence = sequence,
            .payload_length = payload_length,
        };
    }
};

pub const FrameHeadersFlags = packed struct {
    continuation: bool = false,
    compressed: bool = false,
    encrypted: bool = false,
    padding: u5 = 0,
};

pub const FrameType = enum(u4) {
    message,
    handshake,
    ack,
    nack,
    heartbeat,

    pub fn fromBits(bits: u4) !FrameType {
        return switch (bits) {
            0 => .message,
            1 => .handshake,
            2 => .ack,
            3 => .nack,
            4 => .heartbeat,
            else => error.UnsupportedFrameType,
        };
    }
};

pub const FrameParser = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .buffer = .empty,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn parse(self: *Self, frames: []Frame, data: []const u8) !usize {
        // FIX: there should be a constant that is defined that caps the buffer items capacity
        if (self.buffer.items.len + data.len > 10_000) return error.BufferMaxCapacityExceeded;

        try self.buffer.appendSlice(self.allocator, data);

        var count: usize = 0;
        var read_offset: usize = 0;

        while (count < frames.len) {
            const buf = self.buffer.items;

            // FIX: we should do something smart if the frame headers are actually invalid
            const frame = Frame.fromBytes(buf[read_offset..]) catch |err| switch (err) {
                error.Truncated => break,
                error.InvalidFrameHeadersMagic => {
                    read_offset += self.resyncToNextMagic(buf[read_offset..]);

                    // make sure that we don't break something
                    assert(read_offset <= self.buffer.items.len);
                    continue;
                },
                // FIX: this should have all errors handled!
                else => unreachable,
            };

            const bytes_consumed = frame.packedSize();

            // NOTE: we are using asserts here because these would be critical systems failures.
            // I think it would be better to crash instead of trying to recover from this.
            //
            // This would meant that the `Frame.fromBytes` function did something incorrectly
            assert(bytes_consumed > 0);

            // ensure that we have not consumed more things than would b possible
            assert(read_offset + bytes_consumed <= self.buffer.items.len);

            // add the frame to the frames slice
            frames[count] = frame;
            count += 1;
            read_offset += bytes_consumed;
        }

        // if some work was done where bytes were consumed, the advance the self.buffer
        if (read_offset > 0) {
            std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[read_offset..]);
            self.buffer.items.len -= read_offset;
        }

        return count;
    }

    fn resyncToNextMagic(_: *Self, data: []const u8) usize {
        // constants.frame_headers_magic is comprised of 2 bytes (u16) with bigendian ordering;
        // 0x4B42 -> bytes (0x4B 0x42)
        const magic_hi: u8 = (@as(u16, constants.frame_headers_magic) >> 8) & 0xFF;
        const magic_lo: u8 = @as(u8, constants.frame_headers_magic & 0xFF);

        var i: usize = 1;

        // begin searching the data for an instance of magic_hi
        while (i + 1 <= data.len) : (i += 1) {
            const left = data[i - 1];
            const right = data[i];

            if (left == magic_hi and right == magic_lo) {
                // we have found a match for magic and we should resync to this point
                return i - 1;
            }
        }

        return i;
    }
};

pub const Reassembler = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    accumulator: std.ArrayList(u8),
    expected_sequence: u16 = 0,
    is_assembling: bool = false,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .accumulator = .empty,
            .expected_sequence = 0,
            .is_assembling = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.accumulator.deinit(self.allocator);
    }

    pub fn assemble(self: *Self, allocator: std.mem.Allocator, frame: Frame) !?[]const u8 {
        const flags = frame.frame_headers.flags;
        const sequence = frame.frame_headers.sequence;

        // check if we are currently assembling a message
        if (!self.is_assembling) {
            // start assembling a new message
            self.expected_sequence = 0;
            self.is_assembling = true;
        }

        // check if the incoming frame's sequence is in the correct order
        // Hard fail if we did not receive the message in the correct order. The caller can use
        // the error returned and tell the client to retransmit
        if (self.expected_sequence != sequence) {
            self.is_assembling = false;
            self.expected_sequence = 0;
            return error.FrameOutOfOrder;
        }

        // consider this sequence consumed
        self.expected_sequence += 1;

        // this is just another segment of the message
        if (flags.continuation) {
            try self.accumulator.appendSlice(self.allocator, frame.payload);
            return null;
        }

        // if there are no more frames that are part of this message
        if (self.accumulator.items.len == 0) {
            // this is full message inside of a single frame
            var message_bytes = try std.ArrayList(u8).initCapacity(
                allocator,
                frame.payload.len,
            );
            message_bytes.appendSliceAssumeCapacity(frame.payload);

            self.is_assembling = false;
            self.expected_sequence = 0;

            return message_bytes.items;
        } else {
            // this is the final segment of the message
            try self.accumulator.appendSlice(self.allocator, frame.payload);

            // initialize an array list containing the entire message
            var message_bytes = try std.ArrayList(u8).initCapacity(
                allocator,
                self.accumulator.items.len,
            );
            message_bytes.appendSliceAssumeCapacity(self.accumulator.items);

            self.is_assembling = false;
            self.expected_sequence = 0;

            return message_bytes.items;
        }
    }
};

test "frame size" {
    var payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    var frame = Frame.new(&payload, .{});

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    const n = frame.toBytes(&buf);

    try testing.expectEqual(n, frame.packedSize());
}

test "frame header size" {
    var buf: [FrameHeaders.packedSize()]u8 = undefined;

    try testing.expectEqual(FrameHeaders.packedSize(), buf.len);

    const frame_header = FrameHeaders{};

    const n = frame_header.toBytes(&buf);

    try testing.expectEqual(FrameHeaders.packedSize(), n);
}

test "create a frame from bytes" {
    var payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    var before_frame = Frame.new(&payload, .{});

    // convert the frame to bytes
    var buf: [100]u8 = undefined;
    const n = before_frame.toBytes(&buf);

    try testing.expectEqual(before_frame.packedSize(), n);

    // convert the frame from bytes
    const after_frame = try Frame.fromBytes(buf[0..]);

    try testing.expectEqual(after_frame.packedSize(), n);

    // validate that the fields are all the same
    try testing.expectEqual(before_frame.frame_headers.magic, after_frame.frame_headers.magic);
    try testing.expectEqual(
        before_frame.frame_headers.protocol_version,
        after_frame.frame_headers.protocol_version,
    );
    try testing.expectEqual(
        before_frame.frame_headers.frame_type,
        after_frame.frame_headers.frame_type,
    );
    try testing.expectEqual(
        before_frame.frame_headers.flags,
        after_frame.frame_headers.flags,
    );
    try testing.expectEqual(
        before_frame.frame_headers.sequence,
        after_frame.frame_headers.sequence,
    );
    try testing.expectEqual(
        before_frame.frame_headers.payload_length,
        after_frame.frame_headers.payload_length,
    );
    try testing.expect(std.mem.eql(u8, before_frame.payload, after_frame.payload));
    try testing.expectEqual(before_frame.checksum, after_frame.checksum);
}

test "frame parsing happy path" {
    const allocator = testing.allocator;

    var disassembler = FrameParser.init(allocator);
    defer disassembler.deinit();

    // make an arbitrary frame
    var payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    var frame = Frame.new(&payload, .{});

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    const n = frame.toBytes(&buf);

    try testing.expectEqual(n, frame.packedSize());

    var frames: [5]Frame = undefined;

    const frames_parsed = try disassembler.parse(&frames, buf[0..n]);
    try testing.expectEqual(1, frames_parsed);
}

test "frame parse a bad frame and resync to magic" {
    const allocator = testing.allocator;

    var disassembler = FrameParser.init(allocator);
    defer disassembler.deinit();

    // make an arbitrary frame
    var bad_frame_payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    var bad_frame = Frame.new(&bad_frame_payload, .{});
    bad_frame.frame_headers.magic = 1234;

    try testing.expect(constants.frame_headers_magic != bad_frame.frame_headers.magic);

    var good_frame_payload = [_]u8{ 1, 1, 1, 1, 1 };
    var good_frame = Frame.new(&good_frame_payload, .{});

    try testing.expectEqual(constants.frame_headers_magic, good_frame.frame_headers.magic);
    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    const bad_frame_n = bad_frame.toBytes(&buf);
    const good_frame_n = good_frame.toBytes(buf[bad_frame_n..]);

    try testing.expectEqual(bad_frame_n, bad_frame.packedSize());

    var frames: [5]Frame = undefined;

    const frames_parsed = try disassembler.parse(&frames, buf[0 .. bad_frame_n + good_frame_n]);

    try testing.expectEqual(1, frames_parsed);

    // ensure that the good frame was the one that was parsed
    try testing.expect(std.mem.eql(u8, frames[0].payload, good_frame.payload));
}

test "frame parse multiple frames" {
    const allocator = testing.allocator;

    var frame_parser = FrameParser.init(allocator);
    defer frame_parser.deinit();

    var frame_payload = [_]u8{ 1, 1, 1, 1, 1 };
    var frame_1 = Frame.new(&frame_payload, .{});
    var frame_2 = Frame.new(&frame_payload, .{});
    var frame_3 = Frame.new(&frame_payload, .{});

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    // write all three frames to the buffer
    const frame_1_n = frame_1.toBytes(&buf);
    const frame_2_n = frame_2.toBytes(buf[frame_1_n..]);
    _ = frame_3.toBytes(buf[frame_1_n + frame_2_n ..]);

    var frames: [5]Frame = undefined;

    // parse the entire buffer
    const frames_parsed = try frame_parser.parse(&frames, &buf);

    try testing.expectEqual(3, frames_parsed);

    // ensure that the good frame was the one that was parsed
    try testing.expect(std.mem.eql(u8, frames[0].payload, frame_1.payload));
    try testing.expect(std.mem.eql(u8, frames[1].payload, frame_2.payload));
    try testing.expect(std.mem.eql(u8, frames[2].payload, frame_3.payload));

    try testing.expectEqual(0, frame_parser.buffer.items.len);
}

test "reassembly single frame message" {
    const allocator = testing.allocator;

    var frame_parser = FrameParser.init(allocator);
    defer frame_parser.deinit();

    var frame_payload = [_]u8{ 1, 1, 1, 1, 1 };
    var frame = Frame.new(&frame_payload, .{});

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    // write all three frames to the buffer
    const frame_bytes = frame.toBytes(&buf);

    var frames: [5]Frame = undefined;

    // parse the entire buffer
    const frames_parsed = try frame_parser.parse(&frames, buf[0..frame_bytes]);

    var reassembler = Reassembler.init(allocator);
    defer reassembler.deinit();

    for (frames[0..frames_parsed]) |f| {
        if (try reassembler.assemble(allocator, f)) |message_bytes| {
            defer allocator.free(message_bytes);

            try testing.expect(std.mem.eql(u8, &frame_payload, message_bytes));
        }
    }
}

test "reassembly of multi frame message" {
    const allocator = testing.allocator;

    var frame_parser = FrameParser.init(allocator);
    defer frame_parser.deinit();

    var frame_payload = [_]u8{ 1, 1, 1, 1, 1 };
    var frame_1 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = true },
        .sequence = 0,
    });
    var frame_2 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = true },
        .sequence = 1,
    });
    var frame_3 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = false },
        .sequence = 2,
    });

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    // write all three frames to the buffer
    const frame_1_n = frame_1.toBytes(&buf);
    const frame_2_n = frame_2.toBytes(buf[frame_1_n..]);
    _ = frame_3.toBytes(buf[frame_1_n + frame_2_n ..]);

    var frames: [5]Frame = undefined;

    // parse the entire buffer
    const frames_parsed = try frame_parser.parse(&frames, &buf);

    try testing.expectEqual(3, frames_parsed);

    try testing.expectEqual(0, frame_parser.buffer.items.len);

    var reassembler = Reassembler.init(allocator);
    defer reassembler.deinit();

    // I don't think I like this API
    for (frames[0..frames_parsed]) |f| {
        if (try reassembler.assemble(allocator, f)) |message_bytes| {
            std.debug.print("message_bytes: {any}\n", .{message_bytes});

            defer allocator.free(message_bytes);
        }
    }
}

test "reassembly fails if out of order" {
    const allocator = testing.allocator;

    var frame_parser = FrameParser.init(allocator);
    defer frame_parser.deinit();

    var frame_payload = [_]u8{ 1, 1, 1, 1, 1 };
    var frame_1 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = true },
        .sequence = 0,
    });
    var frame_2 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = true },
        .sequence = 1,
    });
    var frame_3 = Frame.new(&frame_payload, .{
        .flags = .{ .continuation = false },
        .sequence = 2,
    });

    // make a buffer that is simply big enough for the entire frame. This could be interpreted
    // as being a `recv_buffer` on a connection
    var buf: [100]u8 = undefined;

    // write all three frames to the buffer
    const frame_1_n = frame_1.toBytes(&buf);
    const frame_2_n = frame_2.toBytes(buf[frame_1_n..]);
    _ = frame_3.toBytes(buf[frame_1_n + frame_2_n ..]);

    var frames: [5]Frame = undefined;

    // parse the entire buffer
    const frames_parsed = try frame_parser.parse(&frames, &buf);

    try testing.expectEqual(3, frames_parsed);

    try testing.expectEqual(0, frame_parser.buffer.items.len);

    var reassembler = Reassembler.init(allocator);
    defer reassembler.deinit();

    try testing.expect(try reassembler.assemble(allocator, frames[0]) == null);

    // we are purposefully sending a frame out of order here
    try testing.expectError(error.FrameOutOfOrder, reassembler.assemble(allocator, frames[2]));
}
