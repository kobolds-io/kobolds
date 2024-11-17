const std = @import("std");
const testing = std.testing;
const utils = @import("./utils.zig");

const TurboShake128 = std.crypto.hash.sha3.TurboShake128;
const Fnv1a_128 = std.hash.Fnv1a_128;
const XxHash3 = std.hash.XxHash3;

const hash_seed: u64 = 0x79810fb604cfd2d7;

pub fn checksumV1(in: []const u8) [16]u8 {
    var out: [16]u8 = undefined;
    TurboShake128(0x1f).hash(in, &out, .{});
    return out;
}

// checksumV2 uses a non cryptographicallys secure hashing algorithm.
// this hashing function doesn't need to be secure because it is used to
// verify the integrity of messages, not to digitally sign anything.
pub fn checksumV2(in: []const u8) u128 {
    return Fnv1a_128.hash(in);
}

// checksumV3 uses a non cryptographicallys secure hashing algorithm.
// this hashing function doesn't need to be secure because it is used to
// verify the integrity of messages, not to digitally sign anything.
pub fn checksumV3(in: []const u8) u64 {
    return XxHash3.hash(hash_seed, in);
}

// verifyV1 if data can hash to the same value.
// returns false if values do not match
// return true if both sums are equal
pub fn verifyV1(sum: [16]u8, data: []const u8) bool {
    return std.mem.eql(u8, &sum, &checksumV1(data));
}

// verifyV2 if data can hash to the same value.
// returns false if values do not match
// return true if both sums are equal
pub fn verifyV2(sum: u128, data: []const u8) bool {
    return sum == checksumV2(data);
}

// verifyV3 if data can hash to the same value.
// returns false if values do not match
// return true if both sums are equal
pub fn verifyV3(sum: u64, data: []const u8) bool {
    return sum == checksumV3(data);
}
