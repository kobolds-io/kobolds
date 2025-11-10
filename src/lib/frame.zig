const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const check = @import("./checksum.zig");

const ProtocolVersion = @import("protocol.zig").ProtocolVersion;

pub const Frame = struct {
    const Self = @This();

    pub const Options = struct {};

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
        _ = opts;

        assert(payload.len <= std.math.maxInt(u16));

        return Self{
            .frame_headers = FrameHeaders{
                .payload_length = @intCast(payload.len),
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
        const checksum = check.checksumCrc32(buf[0..write_offset]);
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
        if (!check.verifyCrc32(checksum, data[0..read_offset])) return error.InvalidChecksum;

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
    magic: u16 = 0x4B42,

    /// The protocol version this frame complies with
    protocol_version: ProtocolVersion = .v1,

    /// The purpose of this frame
    frame_type: FrameType = .message,

    /// Bitmask containing flags describing this frame and its contents
    flags: FrameHeaderFlags = .{},

    /// sequence number of the is frame.
    sequence: u16 = 0,

    /// length of the payload in bytes
    payload_length: u16 = 0,

    /// Returns the number of bytes this struct consumes once serialized to bytes
    pub fn packedSize() usize {
        return @sizeOf(u16) +
            1 + // protocol_version and flags combine to a single byte
            @sizeOf(FrameHeaderFlags) +
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

        const protocol_version_and_frame_type = data[read_offset];
        const protocol_version_bits: u4 = @intCast(protocol_version_and_frame_type >> 4);
        const protocol_version = ProtocolVersion.fromBits(protocol_version_bits);

        const frame_type_bits: u4 = @intCast(protocol_version_and_frame_type & 0xF);
        const frame_type = try FrameType.fromBits(frame_type_bits);

        read_offset += 1;

        const flags: FrameHeaderFlags = @bitCast(data[read_offset]);
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

pub const FrameHeaderFlags = packed struct {
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
