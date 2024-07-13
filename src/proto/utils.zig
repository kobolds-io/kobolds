const std = @import("std");

/// Compares two slices and returns whether they are equal.
pub fn eql(comptime T: type, a: []const T, b: []const T) bool {
    if (a.len != b.len) return false;
    if (a.ptr == b.ptr) return true;
    for (a, b) |a_elem, b_elem| {
        if (a_elem != b_elem) return false;
    }
    return true;
}

pub fn beToU32(bytes: *[4]u8) u32 {
    return std.mem.readInt(u32, bytes, .big);
}

fn u32ToBytes(value: u32) [4]u8 {
    return [_]u8{
        @intCast((value >> 24) & 0xFF),
        @intCast((value >> 16) & 0xFF),
        @intCast((value >> 8) & 0xFF),
        @intCast(value & 0xFF),
    };
}

test beToU32 {
    var buf = [_]u8{ 0, 0, 0, 5, 1, 1, 1, 1, 1 };
    const bytes = buf[0..4];
    const want: u32 = 5;

    const got = beToU32(bytes);

    try std.testing.expectEqual(want, got);
}

test u32ToBytes {
    const value1: u32 = 5;
    const bytes1 = u32ToBytes(value1);
    const want1 = [4]u8{ 0, 0, 0, 5 };

    try std.testing.expect(eql(u8, &want1, &bytes1));

    const value2: u32 = 256;
    const bytes2 = u32ToBytes(value2);
    const want2 = [4]u8{ 0, 0, 1, 0 };

    try std.testing.expect(eql(u8, &want2, &bytes2));

    const value3: u32 = 3000;
    const bytes3 = u32ToBytes(value3);
    std.debug.print("bytes {any}\n", .{bytes3});
    const want3 = [4]u8{ 0, 0, 11, 184 };

    try std.testing.expect(eql(u8, &want3, &bytes3));
}
