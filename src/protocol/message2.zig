const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

pub const Message = struct {
    const Self = @This();

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .undefined = {} },
    body_buffer: [constants.message_max_body_size]u8 = undefined,
    checksum: u64 = 0,
    // checksum: u32 = 0,

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

        // copy v into the body_buffer
        @memcpy(self.body_buffer[0..v.len], v);

        // ensure the header.body_length
        self.fixed_headers.body_length = @intCast(v.len);
    }

    pub fn packedSize(self: Self) usize {
        var sum: usize = 0;
        sum += FixedHeaders.packedSize();
        sum += self.extension_headers.packedSize2();
        sum += self.fixed_headers.body_length;
        sum += @sizeOf(u64);

        return sum;
    }

    pub fn setTopicName(self: *Self, v: []const u8) void {
        switch (self.extension_headers) {
            .publish => |*headers| {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            else => unreachable,
        }
    }

    pub fn topicName(self: *Self) []const u8 {
        switch (self.extension_headers) {
            .publish => |headers| {
                return headers.topic_name[0..headers.topic_name_length];
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

        const fixed_headers_bytes_len = self.fixed_headers.toBytes2(buf[0..]);
        i += fixed_headers_bytes_len;

        const extension_headers_bytes_len = self.extension_headers.toBytes2(buf[i..]);
        i += extension_headers_bytes_len;

        @memcpy(buf[i .. i + self.fixed_headers.body_length], self.body());
        i += self.fixed_headers.body_length;

        // take the entire buffer that has been written and calculate a checksum
        const checksum = hash.xxHash64Checksum(buf[0..i]);
        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], checksum, .big);
        i += @sizeOf(u64);

        // const checksum = hash.checksumCrc32(buf[0..i]);
        // std.mem.writeInt(u32, buf[i..][0..@sizeOf(u32)], checksum, .big);
        // i += @sizeOf(u32);

        return i;
    }

    pub fn deserialize(data: []const u8) !Message {
        // ensure that the buffer is at least the minimum size that a message could possibly be.
        if (data.len < FixedHeaders.packedSize2()) return error.Truncated;

        var i: usize = 0;

        // get the fixed headers from the bytes
        const fixed_headers = try FixedHeaders.fromBytes2(data[0..FixedHeaders.packedSize2()]);
        i += FixedHeaders.packedSize2();

        const extension_headers = try ExtensionHeaders.fromBytes2(fixed_headers.message_type, data[i..]);
        i += extension_headers.packedSize2();

        if (fixed_headers.body_length > constants.message_max_body_size) return error.InvalidMessage;
        if (data[i..].len < fixed_headers.body_length) return error.Truncated;

        var body_buffer: [constants.message_max_body_size]u8 = undefined;
        @memcpy(body_buffer[0..fixed_headers.body_length], data[i .. i + fixed_headers.body_length]);
        i += fixed_headers.body_length;

        if (data[i..].len < @sizeOf(u64)) return error.Truncated;
        const checksum = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);

        if (!hash.xxHash64Verify(checksum, data[0..i])) return error.InvalidChecksum;
        i += @sizeOf(u64);

        // if (data[i..].len < @sizeOf(u32)) return error.Truncated;
        // const checksum = std.mem.readInt(u32, data[i .. i + @sizeOf(u32)][0..@sizeOf(u32)], .big);

        // if (!hash.verifyCrc32(checksum, data[0..i])) return error.InvalidChecksum;
        // i += @sizeOf(u32);

        // TODO: validate the message

        return Message{
            .fixed_headers = fixed_headers,
            .extension_headers = extension_headers,
            .body_buffer = body_buffer,
            .checksum = checksum,
        };
    }
};

pub const FixedHeaders = packed struct {
    const Self = @This();

    comptime {
        assert(6 == Self.packedSize2());
    }

    body_length: u16 = 0,
    message_type: MessageType = .undefined,
    version: u4 = 0,
    flags: u4 = 0,
    padding: u16 = 0,

    pub inline fn packedSize2() usize {
        return 6; // 2 + 1 + 1 + 2
    }

    pub inline fn toBytes2(self: *const Self, buf: []u8) usize {
        // assert buffer is large enough
        std.debug.assert(buf.len >= Self.packedSize2());

        var i: usize = 0;

        // body_length (big endian)
        buf[i] = @intCast(self.body_length >> 8);
        buf[i + 1] = @intCast(self.body_length & 0xFF);
        i += 2;

        // message_type
        buf[i] = @intFromEnum(self.message_type);
        i += 1;

        // version + flags packed into one byte
        buf[i] = (@as(u8, self.version) << 4) | @as(u8, self.flags);
        i += 1;

        // padding (big endian)
        buf[i] = @intCast(self.padding >> 8);
        buf[i + 1] = @intCast(self.padding & 0xFF);
        i += 2;

        return i;
    }

    pub inline fn fromBytes2(data: []const u8) !Self {
        if (data.len < Self.packedSize2()) return error.Truncated;

        var i: usize = 0;

        const body_length = (@as(u16, data[i]) << 8) | @as(u16, data[i + 1]);
        i += 2;

        const message_type: MessageType = switch (data[i]) {
            1 => .publish,
            else => .undefined,
        };
        i += 1;

        const version_flags = data[i];
        const version: u4 = @intCast(version_flags >> 4);
        const flags: u4 = @intCast(version_flags & 0xF);
        i += 1;

        const padding = (@as(u16, data[i]) << 8) | @as(u16, data[i + 1]);
        i += 2;

        return FixedHeaders{
            .body_length = body_length,
            .message_type = message_type,
            .version = version,
            .flags = flags,
            .padding = padding,
        };
    }

    /// Writes the packed struct into `buf` in big-endian order.
    /// Returns the slice of written bytes.
    pub fn toBytes(self: *const Self, buf: []u8) usize {
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

    pub fn packedSize() usize {
        return @sizeOf(Self) - 2;
    }

    pub fn fromBytes(data: []const u8) !FixedHeaders {
        if (data.len < FixedHeaders.packedSize()) return error.Truncated;

        var i: usize = 0;

        const body_length = std.mem.readInt(u16, data[i..][0..2], .big);
        i += 2;

        const message_type: MessageType = switch (data[i]) {
            1 => .publish,
            else => .undefined,
        };
        i += 1;

        const version_flags_byte = data[i];
        const version: u4 = @intCast((version_flags_byte >> 4) & 0xF);
        const flags: u4 = @intCast(version_flags_byte & 0xF);
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

    pub inline fn packedSize2(self: *const Self) usize {
        return switch (self.*) {
            .publish => |headers| 8 + 1 + headers.topic_name_length,
            .undefined => 0,
        };
    }

    pub inline fn toBytes2(self: *const Self, buf: []u8) usize {
        return switch (self.*) {
            .undefined => 0,
            .publish => |headers| {
                var i: usize = 0;

                // message_id big endian
                buf[i] = @intCast((headers.message_id >> 56) & 0xFF);
                buf[i + 1] = @intCast((headers.message_id >> 48) & 0xFF);
                buf[i + 2] = @intCast((headers.message_id >> 40) & 0xFF);
                buf[i + 3] = @intCast((headers.message_id >> 32) & 0xFF);
                buf[i + 4] = @intCast((headers.message_id >> 24) & 0xFF);
                buf[i + 5] = @intCast((headers.message_id >> 16) & 0xFF);
                buf[i + 6] = @intCast((headers.message_id >> 8) & 0xFF);
                buf[i + 7] = @intCast(headers.message_id & 0xFF);
                i += 8;

                // topic_name_length
                buf[i] = headers.topic_name_length;
                i += 1;

                // topic_name bytes
                @memcpy(buf[i .. i + headers.topic_name_length], headers.topic_name[0..headers.topic_name_length]);
                i += @intCast(headers.topic_name_length);

                return i;
            },
        };
    }

    pub inline fn fromBytes2(message_type: MessageType, data: []const u8) !Self {
        return switch (message_type) {
            .publish => blk: {
                var i: usize = 0;

                if (data.len < 9) return error.Truncated; // 8 + 1 minimum

                const message_id =
                    (@as(u64, data[i]) << 56) |
                    (@as(u64, data[i + 1]) << 48) |
                    (@as(u64, data[i + 2]) << 40) |
                    (@as(u64, data[i + 3]) << 32) |
                    (@as(u64, data[i + 4]) << 24) |
                    (@as(u64, data[i + 5]) << 16) |
                    (@as(u64, data[i + 6]) << 8) |
                    (@as(u64, data[i + 7]));
                i += 8;

                const topic_name_length = data[i];
                i += 1;

                if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
                if (i + topic_name_length > data.len) return error.Truncated;

                var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
                @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);

                break :blk ExtensionHeaders{ .publish = PublishHeaders{
                    .message_id = message_id,
                    .topic_name_length = topic_name_length,
                    .topic_name = topic_name,
                } };
            },
            .undefined => ExtensionHeaders{ .undefined = {} },
        };
    }

    pub fn toBytes(self: *Self, buf: []u8) usize {
        return switch (self.*) {
            .undefined => 0,
            .publish => |headers| blk: {
                assert(buf.len >= @sizeOf(PublishHeaders));

                var i: usize = 0;

                std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], headers.message_id, .big);
                i += @sizeOf(u64);

                buf[i] = headers.topic_name_length;
                i += 1;

                @memcpy(buf[i .. i + headers.topic_name_length], headers.topic_name[0..headers.topic_name_length]);
                i += @intCast(headers.topic_name_length);

                break :blk i;
            },
        };
    }

    pub fn fromBytes(message_type: MessageType, data: []const u8) !Self {
        var i: usize = 0;

        return switch (message_type) {
            .undefined => ExtensionHeaders{ .undefined = {} },
            .publish => blk: {
                if (data.len < 8 + 1) return error.Truncated;

                // message_id (u64 big endian)
                const message_id = std.mem.readInt(u64, data[i..][0..@sizeOf(u64)], .big);
                i += @sizeOf(u64);

                // topic_name_length (u8)
                const topic_name_length = data[i];
                i += 1;

                if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
                if (i + topic_name_length > data.len) return error.Truncated;

                // topic_name
                var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
                @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);
                i += topic_name_length;

                break :blk ExtensionHeaders{
                    .publish = PublishHeaders{
                        .message_id = message_id,
                        .topic_name_length = topic_name_length,
                        .topic_name = topic_name,
                    },
                };
            },
        };
    }

    pub fn packedSize(self: Self) usize {
        return switch (self) {
            .undefined => 0,
            .publish => |headers| return @sizeOf(u64) + 1 + headers.topic_name_length,
        };
    }
};

pub const PublishHeaders = struct {
    message_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,
};

test "size of structs" {
    try testing.expectEqual(8, @sizeOf(FixedHeaders));
    try testing.expectEqual(6, FixedHeaders.packedSize2());
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
            // .undefined => try testing.expectEqual(bytes, 10),
            // .publish => try testing.expectEqual(bytes, 19),
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

        switch (message_type) {
            .publish => {
                // message.setBody("hello");
                // message.extension_headers.publish.message_id = 123;
                // message.setTopicName("asdf");
            },
            else => {},
        }

        // serialize the message
        const bytes = message.serialize(&buf);
        // log.err("message_type {any} bytes deserialization {}", .{ message_type, bytes });

        // deserialize the message
        var deserialized_message = try Message.deserialize(buf[0..bytes]);

        try testing.expectEqual(message.size(), deserialized_message.size());
        try testing.expect(std.mem.eql(u8, message.body(), deserialized_message.body()));

        switch (message_type) {
            .publish => {
                try testing.expectEqual(
                    message.extension_headers.publish.message_id,
                    deserialized_message.extension_headers.publish.message_id,
                );
                try testing.expectEqual(
                    message.extension_headers.publish.topic_name_length,
                    deserialized_message.extension_headers.publish.topic_name_length,
                );
                try testing.expect(std.mem.eql(u8, message.topicName(), deserialized_message.topicName()));
            },
            else => {},
        }
    }
}
