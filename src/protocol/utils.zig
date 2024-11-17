const std = @import("std");

const Message = @import("./message.zig").Message;

pub fn u128ToBytes(value: u128) [16]u8 {
    return [_]u8{
        @intCast(value >> 120 & 0xff),
        @intCast(value >> 112 & 0xff),
        @intCast(value >> 104 & 0xff),
        @intCast(value >> 96 & 0xff),
        @intCast(value >> 88 & 0xff),
        @intCast(value >> 80 & 0xff),
        @intCast(value >> 72 & 0xff),
        @intCast(value >> 64 & 0xff),
        @intCast(value >> 56 & 0xff),
        @intCast(value >> 48 & 0xff),
        @intCast(value >> 40 & 0xff),
        @intCast(value >> 32 & 0xff),
        @intCast(value >> 24 & 0xff),
        @intCast(value >> 16 & 0xff),
        @intCast(value >> 8 & 0xff),
        @intCast(value & 0xff),
    };
}

pub fn u64ToBytes(value: u64) [8]u8 {
    return [_]u8{
        @intCast(value >> 56 & 0xff),
        @intCast(value >> 48 & 0xff),
        @intCast(value >> 40 & 0xff),
        @intCast(value >> 32 & 0xff),
        @intCast(value >> 24 & 0xff),
        @intCast(value >> 16 & 0xff),
        @intCast(value >> 8 & 0xff),
        @intCast(value & 0xff),
    };
}

pub fn u32ToBytes(value: u32) [4]u8 {
    return [_]u8{
        @intCast((value >> 24) & 0xFF),
        @intCast((value >> 16) & 0xFF),
        @intCast((value >> 8) & 0xFF),
        @intCast(value & 0xFF),
    };
}

pub fn u16ToBytes(value: u16) [2]u8 {
    return [_]u8{
        @intCast((value >> 8) & 0xff),
        @intCast(value & 0xff),
    };
}
