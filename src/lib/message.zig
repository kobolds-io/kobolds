const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const MessageType = @import("./message_type.zig").MessageType;
const PingHeaders = @import("./headers.zig").PingHeaders;
const PongHeaders = @import("./headers.zig").PongHeaders;

const ChunkReader = @import("./chunk.zig").ChunkReader;

pub const Message = struct {
    const Self = @This();

    pub const Options = struct {};

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .unsupported = {} },
    body_chunks: ?*BodyChunk = null,

    pub fn new(message_type: MessageType, opts: Options) Self {
        _ = opts;

        return switch (message_type) {
            .unsupported => Self{
                .fixed_headers = FixedHeaders{ .message_type = .unsupported },
                .extension_headers = ExtensionHeaders{ .unsupported = {} },
                .body_chunks = null,
            },
            .ping => Self{
                .fixed_headers = FixedHeaders{ .message_type = .ping },
                .extension_headers = ExtensionHeaders{ .ping = PingHeaders{} },
                .body_chunks = null,
            },
            .pong => Self{
                .fixed_headers = FixedHeaders{ .message_type = .pong },
                .extension_headers = ExtensionHeaders{ .pong = PongHeaders{} },
                .body_chunks = null,
            },
        };
    }

    /// Calculate the size of the unserialized message.
    pub fn packedSize(self: Self) usize {
        var body_length: usize = 0;
        var current_chunk = self.body_chunks;
        while (current_chunk) |chunk| {
            body_length += chunk.len;
            if (chunk.next) |next_chunk| {
                current_chunk = next_chunk;
            }
        }

        return FixedHeaders.packedSize() + self.extension_headers.packedSize() + body_length;
    }

    pub fn serialize(self: *Self, buf: []u8) usize {
        // Ensure the buffer is large enough
        assert(buf.len >= self.packedSize());

        var i: usize = 0;

        // Fixed headers
        i += self.fixed_headers.toBytes(buf[i..]);

        // Extension headers
        i += self.extension_headers.toBytes(buf[i..]);

        var current_chunk = self.body_chunks;
        while (current_chunk) |chunk| {
            @memcpy(buf[i .. i + chunk.len], chunk.bytes[0..chunk.len]);
            if (chunk.next) |next_chunk| {
                current_chunk = next_chunk;
            }
        }

        // if this didn't work something went wrong
        assert(self.packedSize() == i);

        return i;
    }

    pub fn deserialize(data: []const u8) !DeserializeResult {
        _ = data;
        // NOTE: all of the data for this message should be contained inside of data?????
        //     if this is true, then we may be potentially doubling the amount of memory needed for
        //     this message. Alternatively, the frame would have more information regarding this
        //     message. The next body_chunk could be a chunk on disk OR it could be in memory.
        //
        //

        // FIX: not implemented
        unreachable;

        // return .{ .message = Message.new(.unsupported, .{}), .bytes_consumed = 0 };
    }

    const DeserializeResult = struct {
        message: Self,
        bytes_consumed: usize,
    };
};

pub const Flags = packed struct {
    const Self = @This();

    padding: u8 = 0,

    pub fn packedSize() usize {
        return 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= Flags.packedSize());

        var i: usize = 0;
        buf[i] = self.padding;
        i += 1;

        return i;
    }
};

pub const FixedHeaders = struct {
    const Self = @This();

    message_type: MessageType = .unsupported,
    flags: Flags = Flags{},

    pub fn packedSize() usize {
        return @sizeOf(MessageType) + @sizeOf(Flags);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= @sizeOf(Self));

        var i: usize = 0;

        buf[i] = @intFromEnum(self.message_type);
        i += @sizeOf(MessageType);

        const flag_bytes = self.flags.toBytes(buf[i .. i + @sizeOf(Flags)]);
        i += flag_bytes;

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.packedSize()) return error.Truncated;

        var i: usize = 0;

        const message_type = MessageType.fromByte(data[i]);
        if (message_type == .unsupported) return error.InvalidMessageType;
        i += 1;

        // get the bits for the flags
        const flags: Flags = @bitCast(data[i]);
        i += 1;

        return FixedHeaders{
            .message_type = message_type,
            .flags = flags,
        };
    }
};

pub const ExtensionHeaders = union(MessageType) {
    const Self = @This();

    unsupported: void,
    ping: PingHeaders,
    pong: PongHeaders,

    pub fn packedSize(self: *const Self) usize {
        return switch (self.*) {
            .unsupported => 0,
            inline else => |headers| headers.packedSize(),
        };
    }

    pub fn toBytes(self: *Self, buf: []u8) usize {
        return switch (self.*) {
            .unsupported => 0,
            inline else => |headers| blk: {
                assert(buf.len >= @sizeOf(@TypeOf(headers)));

                const bytes = headers.toBytes(buf);
                break :blk bytes;
            },
        };
    }

    pub fn fromBytes(message_type: MessageType, data: []const u8) !Self {
        return switch (message_type) {
            .unsupported => ExtensionHeaders{ .unsupported = {} },
            .ping => blk: {
                const headers = try PingHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .ping = headers };
            },
            .pong => blk: {
                const headers = try PongHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .pong = headers };
            },
        };
    }
};

pub const BodyChunk = struct {
    len: u16 = 0,
    bytes: []const u8,
    next: ?*BodyChunk,
};

test "size of structs" {
    try testing.expectEqual(2, @sizeOf(FixedHeaders));
    try testing.expectEqual(2, FixedHeaders.packedSize());

    const unsupported_message = Message.new(.unsupported, .{});
    try testing.expectEqual(2, unsupported_message.packedSize());

    const ping_message = Message.new(.ping, .{});
    try testing.expectEqual(10, ping_message.packedSize());

    const pong_message = Message.new(.pong, .{});
    try testing.expectEqual(10, pong_message.packedSize());
}

test "message can comprise of variable size extensions" {
    const ping_message = Message.new(.ping, .{});
    try testing.expectEqual(ping_message.fixed_headers.message_type, .ping);
}

test "message serialization" {
    const message_types = [_]MessageType{
        .ping,
        .pong,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type, .{});

        const bytes = message.serialize(&buf);

        try testing.expectEqual(bytes, message.packedSize());
    }
}

// test "message deserialization" {
//     const message_types = [_]MessageType{
//         .ping,
//         .pong,
//     };

//     var buf: [@sizeOf(Message)]u8 = undefined;

//     for (message_types) |message_type| {
//         var message = Message.new(message_type, .{});

//         // serialize the message
//         const bytes = message.serialize(&buf);

//         // deserialize the message
//         const deserialized_result = try Message.deserialize(buf[0..bytes]);

//         try testing.expectEqual(bytes, deserialized_result.bytes_consumed);

//         var deserialized_message = deserialized_result.message;

//         try testing.expectEqual(message.packedSize(), deserialized_message.packedSize());

//         // FIX: this should compare the body of each message and ensure they are the same
//         // try testing.expect(std.mem.eql(u8, message.body(), deserialized_message.body()));

//         switch (message_type) {
//             .unsupported => {
//                 try testing.expectEqual(message.packedSize(), deserialized_message.packedSize());
//             },
//             .ping => {
//                 try testing.expectEqual(
//                     message.extension_headers.ping.transaction_id,
//                     deserialized_message.extension_headers.ping.transaction_id,
//                 );
//             },
//             .pong => {
//                 try testing.expectEqual(
//                     message.extension_headers.pong.transaction_id,
//                     deserialized_message.extension_headers.pong.transaction_id,
//                 );
//             },
//         }
//     }
// }
