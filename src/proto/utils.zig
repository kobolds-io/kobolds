const std = @import("std");
const cbor = @import("zbor");

const Message = @import("./message.zig").Message;

/// converts a Message to bytes
pub fn serialize(buf: *std.ArrayList(u8), msg: Message) !void {
    // assert that the buffer is empty
    // TODO: make this a ProtoError
    if (buf.items.len > 0) return error.BufferNotEmpty;

    // Convert the message to CBOR
    try msg.cborStringify(.{}, buf.writer());

    // the the message length prefix
    const msg_length_prefix = u32ToBytes(@intCast(buf.items.len));

    // Insert the length prefix at the beginning of the buffer
    try buf.insertSlice(0, &msg_length_prefix);
}

test serialize {
    const allocator = std.testing.allocator;
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    const msg = Message.new("/hello", "world");

    try std.testing.expectEqual(0, buf.items.len);

    try serialize(&buf, msg);

    try std.testing.expect(buf.items.len >= 4);

    // read the first 4 bytes for the length prefix
    // const msg_length_prefix_bytes = buf.items[0..4];
    const msg_length_prefix = bytesToU32(buf.items[0..4]);

    // expect that the
    try std.testing.expect(buf.items.len == 4 + msg_length_prefix);
    try std.testing.expectEqual(
        msg_length_prefix,
        buf.items[4 .. 4 + msg_length_prefix].len,
    );
}

/// converts bytes to a Message
pub fn deserialize(allocator: std.mem.Allocator, bytes: []const u8) !Message {
    const di: cbor.DataItem = try cbor.DataItem.new(bytes);
    return try Message.cborParse(di, .{ .allocator = allocator });
}

pub fn bytesToU32(bytes: *[4]u8) u32 {
    return std.mem.readInt(u32, bytes, .big);
}

test bytesToU32 {
    var buf = [_]u8{ 0, 0, 0, 5 };
    // const bytes = buf[0..4];
    const want: u32 = 5;

    const got = bytesToU32(&buf);

    try std.testing.expectEqual(want, got);
}

pub fn u32ToBytes(value: u32) [4]u8 {
    return [_]u8{
        @intCast((value >> 24) & 0xFF),
        @intCast((value >> 16) & 0xFF),
        @intCast((value >> 8) & 0xFF),
        @intCast(value & 0xFF),
    };
}

test u32ToBytes {
    const value1: u32 = 5;
    const bytes1 = u32ToBytes(value1);
    const want1 = [4]u8{ 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u32 = 256;
    const bytes2 = u32ToBytes(value2);
    const want2 = [4]u8{ 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u32 = 3000;
    const bytes3 = u32ToBytes(value3);
    const want3 = [4]u8{ 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}
