const std = @import("std");
const testing = std.testing;
const utils = @import("./utils.zig");

const XxHash3 = std.hash.XxHash3;
const hash_seed: u64 = 0x79810fb604cfd2d7;

// checksumV3 uses a non cryptographicallys secure hashing algorithm.
// this hashing function doesn't need to be secure because it is used to
// verify the integrity of messages, not to digitally sign anything.
pub fn checksum(in: []const u8) u128 {
    return XxHash3.hash(hash_seed, in);
}

// verifyV3 if data can hash to the same value.
// returns false if values do not match
// return true if both sums are equal
pub fn verify(sum: u128, data: []const u8) bool {
    return sum == checksum(data);
}

test checksum {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u64 = 17401786602027281750;
    const test_2_want: u64 = 8970200394434375128;

    try testing.expectEqual(test_1_want, checksum(&test_1));
    try testing.expectEqual(test_2_want, checksum(&test_2));
}

test verify {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 8, 7, 6, 5, 4, 3, 2, 1, 1, 2, 3, 4, 5, 6, 7, 8 };
    const test_1_want: u64 = 17401786602027281750;
    const test_2_want: u64 = 8970200394434375128;

    try testing.expect(verify(test_1_want, &test_1));
    try testing.expect(verify(test_2_want, &test_2));
}
