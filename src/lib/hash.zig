const std = @import("std");
const testing = std.testing;
const utils = @import("./utils.zig");

const crc = std.hash.crc.Crc32;
const XxHash3 = std.hash.XxHash3;
const XxHash32 = std.hash.XxHash32;

const xxhash_32_hash_seed: u32 = 0x79810fb6;
pub const xxhash_64_hash_seed: u64 = 0x79810fb604cfd2d7;

pub fn xxHash64Checksum(in: []const u8) u64 {
    return XxHash3.hash(xxhash_64_hash_seed, in);
}

pub fn xxHash64Verify(sum: u64, data: []const u8) bool {
    return sum == xxHash64Checksum(data);
}

pub fn xxHash32Checksum(in: []const u8) u32 {
    return XxHash32.hash(xxhash_32_hash_seed, in);
}

pub fn xxHash32Verify(sum: u32, data: []const u8) bool {
    return sum == xxHash32Checksum(data);
}

pub fn checksumCrc32(in: []const u8) u32 {
    var c = crc.init();
    c.update(in);
    return c.final();
}

pub fn verifyCrc32(sum: u32, data: []const u8) bool {
    return sum == checksumCrc32(data);
}

test xxHash64Checksum {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u64 = 17401786602027281750;
    const test_2_want: u64 = 8970200394434375128;

    try testing.expectEqual(test_1_want, xxHash64Checksum(&test_1));
    try testing.expectEqual(test_2_want, xxHash64Checksum(&test_2));
}

test xxHash64Verify {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u64 = 17401786602027281750;
    const test_2_want: u64 = 8970200394434375128;

    try testing.expect(xxHash64Verify(test_1_want, &test_1));
    try testing.expect(xxHash64Verify(test_2_want, &test_2));
}

test checksumCrc32 {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u32 = 3971697493;
    const test_2_want: u32 = 851897609;

    try testing.expectEqual(test_1_want, checksumCrc32(&test_1));
    try testing.expectEqual(test_2_want, checksumCrc32(&test_2));
}

test verifyCrc32 {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u32 = 3971697493;
    const test_2_want: u32 = 851897609;

    try testing.expect(verifyCrc32(test_1_want, &test_1));
    try testing.expect(verifyCrc32(test_2_want, &test_2));
}
