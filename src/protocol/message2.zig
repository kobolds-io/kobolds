const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

const MAX_MESSAGE_SIZE = @sizeOf(FixedHeaders) + @sizeOf(ExtensionHeaders) + constants.message_max_body_size + @alignOf(u64);

pub const Message = struct {
    const Self = @This();
    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .undefined = {} },
    body_buffer: [constants.message_max_body_size]u8 = undefined,
    checksum: u64 = 0,

    pub fn new(message_type: MessageType) Self {
        return switch (message_type) {
            .undefined => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension_headers = .{ .undefined = {} },
                .body_buffer = undefined,
                .checksum = 0,
            },
            .publish => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension_headers = .{ .publish = PublishHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
            },
        };
    }

    pub fn body(self: *Self) []const u8 {
        return self.body_buffer[0..self.fixed_headers.body_length];
    }

    /// copy a slice of bytes into the body_buffer of the message
    pub fn setBody(self: *Self, v: []const u8) void {
        assert(v.len <= constants.message_max_body_size);

        switch (self.fixed_headers.message_type) {
            .publish => {
                // copy v into the body_buffer
                @memcpy(self.body_buffer[0..v.len], v);

                // ensure the header.body_length
                self.fixed_headers.body_length = @intCast(v.len);
            },
            else => |message_type| {
                log.err("invalid operation due to incompatible message_type: {any}", .{message_type});
                @panic("invalid operation");
            },
        }
    }

    pub fn setTopicName(self: *Self, v: []const u8) void {
        // This is an absolutely tedious way of handling setting fields.
        switch (self.fixed_headers.message_type) {
            .publish => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                @memcpy(self.extension_headers.publish.topic_name[0..v.len], v);
                self.extension_headers.publish.topic_name_length = @intCast(v.len);
            },
            else => unreachable,
        }
    }

    /// Calculate the size of the unserialized message.
    pub fn size(self: Self) usize {
        const extension_size: usize = switch (self.fixed_headers.message_type) {
            .undefined => 0,
            .publish => @sizeOf(PublishHeaders),
        };

        return @sizeOf(FixedHeaders) + extension_size + self.fixed_headers.body_length + @sizeOf(u64);
    }

    pub fn serialize(self: *Self, buf: []u8) usize {
        assert(buf.len >= self.size());

        var i: usize = 0;

        const fixed_headers_bytes_len = self.fixed_headers.asBytes(buf[0..]);
        i += fixed_headers_bytes_len;

        const extension_headers_bytes_len = self.extension_headers.asBytes(buf[i..]);
        i += extension_headers_bytes_len;

        @memcpy(buf[i .. i + self.fixed_headers.body_length], self.body());
        i += self.fixed_headers.body_length;

        // take the entire buffer that has been written and calculate a checksum
        const checksum = hash.xxHash64Checksum(buf[0..i]);
        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], checksum, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn deserialize(data: []u8) !Message {
        // ensure that the buffer is at least the minimum size that a message could
        // possibly be.
        if (data.len < FixedHeaders.packed_size) return error.Truncated;

        var i: usize = 0;

        // get the fixed headers from the bytes
        const fixed_headers = FixedHeaders.fromBytes(data[0..FixedHeaders.packed_size]);
        i += FixedHeaders.packed_size;

        log.err("fixed headers {any}", .{fixed_headers});

        // FIX: this is a stub
        return Message.new(.undefined);
    }
};

pub const FixedHeaders = packed struct {
    const Self = @This();
    const packed_size = @sizeOf(Self) - 2;

    comptime {
        assert(6 == packed_size);
    }

    body_length: u16 = 0,
    message_type: MessageType = .undefined,
    version: u4 = 0,
    flags: u4 = 0,
    padding: u16 = 0,

    /// Writes the packed struct into `buf` in big-endian order.
    /// Returns the slice of written bytes.
    pub fn asBytes(self: *const Self, buf: []u8) usize {
        std.debug.assert(buf.len >= @sizeOf(Self));

        var i: usize = 0;

        // body_length (u16 big endian)
        std.mem.writeInt(u16, buf[i..][0..2], self.body_length, .big);
        i += 2;

        // message_type (u8)
        buf[i] = @intFromEnum(self.message_type);
        i += 1;

        // version (u4) + flags (u4) packed into one byte
        buf[i] = (@as(u8, self.version) << 4) | @as(u8, self.flags);
        i += 1;

        // padding (u16 big endian)
        std.mem.writeInt(u16, buf[i..][0..2], self.padding, .big);
        i += 2;

        return i;
    }

    pub fn fromBytes(data: []u8) !FixedHeaders {
        if (data.len < FixedHeaders.packed_size) {
            return error.Truncated;
        }

        var i: usize = 0;

        const body_length = std.mem.readInt(u16, data[i..][0..2], .big);
        i += 2;

        const message_type: MessageType = switch (data[i]) {
            1 => .publish,
            else => .undefined,
        };
        i += 1;

        const vf = data[i];
        const version: u4 = @intCast((vf >> 4) & 0xF);
        const flags: u4 = @intCast(vf & 0xF);
        i += 1;

        const padding = std.mem.readInt(u16, data[i..][0..2], .big);
        i += 2;

        return FixedHeaders{
            .body_length = body_length,
            .message_type = message_type,
            .version = version,
            .flags = flags,
            .padding = padding,
        };
    }
};

pub const MessageType = enum(u8) {
    undefined,
    publish,
};

pub const ExtensionHeaders = union(MessageType) {
    const Self = @This();
    undefined: void,
    publish: PublishHeaders,

    pub fn asBytes(self: *Self, buf: []u8) usize {
        return switch (self.*) {
            .undefined => 0,
            .publish => |headers| blk: {
                assert(buf.len >= @sizeOf(PublishHeaders));

                var i: usize = 0;

                // write the message_id (u64)
                std.mem.writeInt(u64, buf[i..][0..8], headers.message_id, .big);
                i += 8;

                // write the topic_name_length (u8)
                buf[i] = headers.topic_name_length;
                i += 1;

                // write the topic_name ([]const u8)
                @memcpy(buf[i .. i + headers.topic_name_length], headers.topic_name[0..headers.topic_name_length]);
                i += @intCast(headers.topic_name_length);

                break :blk i;
                // break :blk writeStructBigEndian(PublishHeaders, headers, buf);
            },
        };
    }
};

pub const PublishHeaders = struct {
    message_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,
};

test "size of structs" {
    // FixedHeader should be 8 bytes
    try testing.expectEqual(8, @sizeOf(FixedHeaders));

    // 8 from FixedHeaders, 4 from max size of Extension Header
    try testing.expectEqual(MAX_MESSAGE_SIZE, @sizeOf(Message));
}

test "message can comprise of variable size extensions" {
    const publish_message = Message.new(.publish);
    try testing.expectEqual(publish_message.fixed_headers.message_type, .publish);
}

test "message size" {
    var undefined_message = Message.new(.undefined);
    try testing.expectEqual(@sizeOf(FixedHeaders) + 0 + @sizeOf(u64), undefined_message.size());

    var publish_message = Message.new(.publish);
    try testing.expectEqual(@sizeOf(FixedHeaders) + @sizeOf(PublishHeaders) + @sizeOf(u64), publish_message.size());
}

test "message serialization" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        const bytes = message.serialize(&buf);

        try testing.expect(bytes < message.size());

        switch (message_type) {
            .undefined => try testing.expectEqual(bytes, 14),
            .publish => try testing.expectEqual(bytes, 23),
        }
    }
}

test "message deserialization" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        // serialize the message
        const bytes = message.serialize(&buf);

        // deserialize the message
        const deserialized_message = try Message.deserialize(buf[0..bytes]);
        _ = deserialized_message;
        // log.err("deserialized message {any}", .{deserialized_message});
    }
}
