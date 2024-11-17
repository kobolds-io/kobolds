const std = @import("std");
const utils = @import("./utils.zig");

const u16ToBytes = utils.u16ToBytes;
const u32ToBytes = utils.u32ToBytes;
const u64ToBytes = utils.u64ToBytes;
const u128ToBytes = utils.u128ToBytes;

test u16ToBytes {
    const value1: u16 = 5;
    const bytes1 = u16ToBytes(value1);
    const want1 = [2]u8{ 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u16 = 256;
    const bytes2 = u16ToBytes(value2);
    const want2 = [2]u8{ 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u16 = 3000;
    const bytes3 = u16ToBytes(value3);
    const want3 = [2]u8{ 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
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

test u64ToBytes {
    const value1: u64 = 5;
    const bytes1 = u64ToBytes(value1);
    const want1 = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u64 = 256;
    const bytes2 = u64ToBytes(value2);
    const want2 = [8]u8{ 0, 0, 0, 0, 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u64 = 3000;
    const bytes3 = u64ToBytes(value3);
    const want3 = [8]u8{ 0, 0, 0, 0, 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}

test u128ToBytes {
    const value1: u128 = 5;
    const bytes1 = u128ToBytes(value1);
    const want1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u128 = 256;
    const bytes2 = u128ToBytes(value2);
    const want2 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u128 = 3000;
    const bytes3 = u128ToBytes(value3);
    const want3 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}
