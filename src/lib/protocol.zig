const std = @import("std");
const testing = std.testing;

pub const ProtocolVersion = enum(u4) {
    unsupported,
    v1,

    pub fn fromBits(bits: u4) ProtocolVersion {
        return switch (bits) {
            1 => .v1,
            else => .unsupported,
        };
    }
};

test "protocol version fromBits" {
    const unsupported_bits: u4 = 0;
    try testing.expectEqual(.unsupported, ProtocolVersion.fromBits(unsupported_bits));

    const v1_bits: u4 = 1;
    try testing.expectEqual(.v1, ProtocolVersion.fromBits(v1_bits));

    const unknown_bits: u4 = 15;
    try testing.expectEqual(.unsupported, ProtocolVersion.fromBits(unknown_bits));
}
