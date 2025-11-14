const MessageType = @import("./message_type.zig").MessageType;

pub const Message = struct {
    const Self = @This();

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .unsupported = {} },
    body_chunks: *?BodyChunk = null,
};

pub const FixedHeaders = struct {};

pub const ExtensionHeaders = union(MessageType) {};

pub const BodyChunk = struct {
    len: u16 = 0,
    bytes: []const u8,
};
