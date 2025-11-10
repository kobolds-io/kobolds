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
