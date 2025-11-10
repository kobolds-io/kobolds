const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const hash = @import("../hash.zig");

const ProtocolVersion = @import("protocol.zig").ProtocolVersion;

pub const Frame = struct {
    const Self = @This();

    pub const Options = struct {};

    frame_headers: FrameHeaders = .{},
    payload: []u8,

    pub fn packedSize(self: Self) usize {
        var payload_len: usize = 0;
        payload_len = self.payload.len;

        return FrameHeaders.packedSize() + payload_len;
    }

    pub fn new(payload: []u8, opts: Options) Self {
        // NOTE: for now, we just discard the options As features are implemented for each flag,
        // they will be handled.
        _ = opts;

        assert(payload.len <= std.math.maxInt(u16));

        return Self{
            .frame_headers = FrameHeaders{
                .payload_length = @intCast(payload.len),
            },
            .payload = payload,
        };
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());

        var i: usize = 0;
        const frame_header_bytes = self.frame_headers.toBytes(buf);
        i += frame_header_bytes;

        @memcpy(buf[i .. i + self.payload.len], self.payload);
        i += self.payload.len;

        return i;
    }

    fn toChecksumPayload(self: Self, buf: []u8) []u8 {
        assert(buf.len <= self.packedSize() - @sizeOf(self.frame_headers.checksum));
    }

    pub fn fromBytes(data: []u8) !Self {
        _ = data;

        return error.NotImplemented;
        // return Self{};
    }
};

pub const FrameHeaders = struct {
    const Self = @This();

    comptime {
        // if this ever changes we will explode during compile time
        assert(12 == FrameHeaders.packedSize());
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

    /// Checksum of the frame header + payload
    checksum: u32 = 0,

    pub fn packedSize() usize {
        return @sizeOf(u16) +
            1 + // protocol_version and flags combine to a single byte
            @sizeOf(FrameHeaderFlags) +
            @sizeOf(u16) +
            @sizeOf(u16) +
            @sizeOf(u32);
    }

    /// Write the bytes that make up the FrameHeader to the buffer.
    /// [magic, protocol_version + frame_type, flags, sequence, payload_length, checksum]
    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= FrameHeaders.packedSize());

        var i: usize = 0;

        std.mem.writeInt(u16, buf[i..][0..@sizeOf(u16)], self.magic, .big);
        i += @sizeOf(u16);

        // both protocol_version and frame_type are u4s so we combine them into a u8
        buf[i] = (@as(u8, @intFromEnum(self.protocol_version)) << 4) | @as(u8, @intFromEnum(self.frame_type));
        i += 1;

        buf[i] = @as(u8, @bitCast(self.flags));
        i += 1;

        std.mem.writeInt(u16, buf[i..][0..@sizeOf(u16)], self.sequence, .big);
        i += @sizeOf(u16);

        std.mem.writeInt(u16, buf[i..][0..@sizeOf(u16)], self.payload_length, .big);
        i += @sizeOf(u16);

        // figure out the checksum
        const checksum = hash.checksumCrc32(buf[0..i]);
        std.mem.writeInt(u32, buf[i..][0..@sizeOf(u32)], self.checksum, .big);
        i += @sizeOf(u32);

        return i;
    }

    /// Read the bytes that make up a FrameHeader from the buffer.
    /// [magic, protocol_version + frame_type, flags, sequence, payload_length, checksum]
    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        const magic = std.mem.readInt(u16, data[i..][0..@sizeOf(u16)], .big);
        i += @sizeOf(u16);

        const protocol_version_and_frame_type = data[i];
        const protocol_version_bits: u4 = @intCast(protocol_version_and_frame_type >> 4);
        const protocol_version = ProtocolVersion.fromBits(protocol_version_bits);

        const frame_type_bits: u4 = @intCast(protocol_version_and_frame_type >> 0xF);
        const frame_type = FrameType.fromBits(frame_type_bits);

        i += 1;

        const flags: FrameHeaderFlags = @bitCast(@as(u4, data[i] & 0xF));
        i += 1;

        const sequence = std.mem.readInt(u16, data[i..][0..@sizeOf(u16)], .big);
        i += @sizeOf(u16);

        const payload_length = std.mem.readInt(u16, data[i..][0..@sizeOf(u16)], .big);
        i += @sizeOf(u16);

        const checksum = std.mem.readInt(u32, data[i..][0..@sizeOf(u32)], .big);
        i += @sizeOf(u32);

        return FrameHeaders{
            .magic = magic,
            .protocol_version = protocol_version,
            .frame_type = frame_type,
            .flags = flags,
            .sequence = sequence,
            .payload_length = payload_length,
            .checksum = checksum,
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
    const Self = @This();

    message,
    handshake,
    ack,
    nack,
    heartbeat,

    pub fn fromBits(bits: u4) Self {
        return switch (bits) {
            0 => .message,
            1 => .handshake,
            2 => .ack,
            3 => .nack,
            4 => .heartbeat,
        };
    }
};

test "frame size" {
    var payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    const frame = Frame.new(&payload, .{});

    var buf: [@sizeOf(Frame)]u8 = undefined;

    const n = frame.toBytes(&buf);

    try testing.expectEqual(n, frame.packedSize());
}

test "frame header size" {
    var buf: [FrameHeaders.packedSize()]u8 = undefined;

    try testing.expectEqual(12, buf.len);

    const frame_header = FrameHeaders{};

    const n = frame_header.toBytes(&buf);

    try testing.expectEqual(12, n);
}

test "create a frame from bytes" {
    var payload = [_]u8{ 1, 2, 3, 4, 5, 6 };
    const before_frame = Frame.new(&payload, .{});

    var buf: [@sizeOf(Frame)]u8 = undefined;

    const n = before_frame.toBytes(&buf);

    std.debug.print("bytes: {any}\n", .{buf[0..n]});

    const after_frame = try Frame.fromBytes(buf[0..n]);
    std.debug.print("frame: {any}\n", .{after_frame});

    // pull the data from the bytes

}
