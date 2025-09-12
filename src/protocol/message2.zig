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

        // copy v into the body_buffer
        @memcpy(self.body_buffer[0..v.len], v);

        // ensure the header.body_length
        self.fixed_headers.body_length = @intCast(v.len);

        // switch (self.fixed_headers.message_type) {
        //     .publish => {
        //         // copy v into the body_buffer
        //         @memcpy(self.body_buffer[0..v.len], v);

        //         // ensure the header.body_length
        //         self.fixed_headers.body_length = @intCast(v.len);
        //     },
        //     else => |message_type| {
        //         log.err("invalid operation due to incompatible message_type: {any}", .{message_type});
        //         @panic("invalid operation");
        //     },
        // }
    }

    pub fn packedSize(self: Self) usize {
        var sum: usize = 0;
        sum += FixedHeaders.packedSize();
        sum += self.extension_headers.packedSize();
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

        const fixed_headers_bytes_len = self.fixed_headers.toBytes(buf[0..]);
        i += fixed_headers_bytes_len;

        const extension_headers_bytes_len = self.extension_headers.toBytes(buf[i..]);
        i += extension_headers_bytes_len;

        @memcpy(buf[i .. i + self.fixed_headers.body_length], self.body());
        i += self.fixed_headers.body_length;

        // take the entire buffer that has been written and calculate a checksum
        const checksum = hash.xxHash64Checksum(buf[0..i]);
        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], checksum, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn deserialize(data: []const u8) !Message {
        // ensure that the buffer is at least the minimum size that a message could possibly be.
        if (data.len < FixedHeaders.packedSize()) return error.Truncated;

        var i: usize = 0;

        // get the fixed headers from the bytes
        const fixed_headers = try FixedHeaders.fromBytes(data[0..FixedHeaders.packedSize()]);
        i += FixedHeaders.packedSize();

        const extension_headers = try ExtensionHeaders.fromBytes(fixed_headers.message_type, data[i..]);
        i += extension_headers.packedSize();

        if (fixed_headers.body_length > constants.message_max_body_size) return error.InvalidMessage;
        if (data[i..].len < fixed_headers.body_length) return error.Truncated;

        var body_buffer: [constants.message_max_body_size]u8 = undefined;
        @memcpy(body_buffer[0..fixed_headers.body_length], data[i .. i + fixed_headers.body_length]);
        i += fixed_headers.body_length;

        if (data[i..].len < @sizeOf(u64)) return error.Truncated;
        const checksum = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);

        if (!hash.xxHash64Verify(checksum, data[0..i])) return error.InvalidChecksum;
        i += @sizeOf(u64);

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
        assert(6 == Self.packedSize());
    }

    body_length: u16 = 0,
    message_type: MessageType = .undefined,
    version: u4 = 0,
    flags: u4 = 0,
    padding: u16 = 0,

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
