const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

pub const DeserializeResult = struct {
    message: Message,
    bytes_consumed: usize,
};

pub const Message = struct {
    const Self = @This();

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .undefined = {} },
    body_buffer: [constants.message_max_body_size]u8 = undefined,
    checksum: u64 = 0,

    pub fn new(message_type: MessageType) Self {
        return switch (message_type) {
            .undefined => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .undefined = {} },
                .body_buffer = undefined,
                .checksum = 0,
            },
            .publish => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .publish = PublishHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
            },
            .subscribe => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .subscribe = SubscribeHeaders{} },
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
            .publish => |headers| return headers.topic_name[0..headers.topic_name_length],
            .subscribe => |headers| return headers.topic_name[0..headers.topic_name_length],
            else => unreachable,
        }
    }

    /// Calculate the size of the unserialized message.
    pub fn size(self: Self) usize {
        const extension_size: usize = switch (self.fixed_headers.message_type) {
            .undefined => 0,
            .publish => @sizeOf(PublishHeaders),
            .subscribe => @sizeOf(SubscribeHeaders),
        };

        return @sizeOf(FixedHeaders) + extension_size + self.fixed_headers.body_length + @sizeOf(u64);
    }

    //// UPDATED VERSION
    pub fn serialize(self: *Self, buf: []u8) usize {
        // Ensure the buffer is large enough
        assert(buf.len >= self.packedSize());

        var i: usize = 0;

        // Fixed headers
        i += self.fixed_headers.toBytes(buf[i..]);

        // Extension headers
        i += self.extension_headers.toBytes(buf[i..]);

        const body_len = self.fixed_headers.body_length;
        @memcpy(buf[i .. i + body_len], self.body_buffer[0..body_len]);
        i += body_len;

        // Compute checksum directly on the written slice
        const checksum = hash.xxHash64Checksum(buf[0..i]);

        // Write checksum in-place (avoids creating buf[i..][0..size])
        std.mem.writeInt(u64, buf.ptr[i..][0..@sizeOf(u64)], checksum, .big);
        i += @sizeOf(u64);

        // if this didn't work something went wrong
        assert(self.packedSize() == i);

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

        return Message{
            .fixed_headers = fixed_headers,
            .extension_headers = extension_headers,
            .body_buffer = body_buffer,
            .checksum = checksum,
        };
    }

    pub fn deserialize2(data: []const u8) !DeserializeResult {
        // ensure that the buffer is at least the minimum size that a message could possibly be.
        if (data.len < FixedHeaders.packedSize()) return error.Truncated;

        var read_offset: usize = 0;

        // get the fixed headers from the bytes
        const fixed_headers = try FixedHeaders.fromBytes(data[0..FixedHeaders.packedSize()]);
        read_offset += FixedHeaders.packedSize();

        const extension_headers = try ExtensionHeaders.fromBytes(
            fixed_headers.message_type,
            data[FixedHeaders.packedSize()..],
        );
        const extension_headers_size = extension_headers.packedSize();
        read_offset += extension_headers_size;

        const total_message_size =
            FixedHeaders.packedSize() +
            extension_headers_size +
            fixed_headers.body_length +
            @sizeOf(u64);

        if (data.len < total_message_size) return error.Truncated;

        if (fixed_headers.body_length > constants.message_max_body_size) return error.InvalidMessage;
        if (data[read_offset..].len < fixed_headers.body_length) return error.Truncated;

        var body_buffer: [constants.message_max_body_size]u8 = undefined;
        @memcpy(body_buffer[0..fixed_headers.body_length], data[read_offset .. read_offset + fixed_headers.body_length]);
        read_offset += fixed_headers.body_length;

        if (data[read_offset..].len < @sizeOf(u64)) return error.Truncated;
        const checksum = std.mem.readInt(u64, data[read_offset .. read_offset + @sizeOf(u64)][0..@sizeOf(u64)], .big);

        if (!hash.xxHash64Verify(checksum, data[0..read_offset])) return error.InvalidChecksum;
        read_offset += @sizeOf(u64);

        assert(read_offset == total_message_size);

        return DeserializeResult{
            .message = Message{
                .fixed_headers = fixed_headers,
                .extension_headers = extension_headers,
                .body_buffer = body_buffer,
                .checksum = checksum,
            },
            .bytes_consumed = read_offset,
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

    pub fn packedSize() usize {
        return @sizeOf(u16) + @sizeOf(MessageType) + 1 + @sizeOf(u16);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= @sizeOf(Self));

        var i: usize = 0;

        std.mem.writeInt(u16, buf[i..][0..2], self.body_length, .big);
        i += @sizeOf(u16);

        buf[i] = @intFromEnum(self.message_type);
        i += 1;

        // version (u4) + flags (u4) packed into one byte
        buf[i] = (@as(u8, self.version) << 4) | @as(u8, self.flags);
        i += 1;

        std.mem.writeInt(u16, buf[i..][0..2], self.padding, .big);
        i += @sizeOf(u16);

        return i;
    }

    /// Writes the packed struct into `buf` in big-endian order.
    /// Returns the slice of written bytes.
    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.packedSize()) return error.Truncated;

        var i: usize = 0;

        const body_length = (@as(u16, data[i]) << 8) | @as(u16, data[i + 1]);
        i += 2;

        const message_type: MessageType = switch (data[i]) {
            0 => .undefined,
            1 => .publish,
            2 => .subscribe,
            else => return error.InvalidMessageType,
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
};

pub const MessageType = enum(u8) {
    undefined,
    publish,
    subscribe,
};

pub const ExtensionHeaders = union(MessageType) {
    const Self = @This();
    undefined: void,
    publish: PublishHeaders,
    subscribe: SubscribeHeaders,

    pub fn packedSize(self: *const Self) usize {
        return switch (self.*) {
            .undefined => 0,
            inline else => |headers| headers.packedSize(),
        };
    }

    pub fn toBytes(self: *Self, buf: []u8) usize {
        return switch (self.*) {
            .undefined => 0,
            inline else => |headers| blk: {
                assert(buf.len >= @sizeOf(@TypeOf(headers)));

                const bytes = headers.toBytes(buf);
                break :blk bytes;
            },
        };
    }

    pub fn fromBytes(message_type: MessageType, data: []const u8) !Self {
        return switch (message_type) {
            .undefined => ExtensionHeaders{ .undefined = {} },
            .publish => blk: {
                const headers = try PublishHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .publish = headers };
            },
            .subscribe => blk: {
                const headers = try SubscribeHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .subscribe = headers };
            },
        };
    }
};

pub const PublishHeaders = struct {
    const Self = @This();

    message_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,

    pub fn packedSize(self: Self) usize {
        return Self.minimumSize() + self.topic_name_length;
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.message_id, .big);
        i += @sizeOf(u64);

        buf[i] = self.topic_name_length;
        i += 1;

        @memcpy(buf[i .. i + self.topic_name_length], self.topic_name[0..self.topic_name_length]);
        i += @intCast(self.topic_name_length);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const message_id = std.mem.readInt(u64, data[0..8], .big);
        i += 8;

        const topic_name_length = data[i];
        i += 1;

        if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
        if (i + topic_name_length > data.len) return error.Truncated;

        var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
        @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);

        return Self{
            .message_id = message_id,
            .topic_name_length = topic_name_length,
            .topic_name = topic_name,
        };
    }
};

pub const SubscribeHeaders = struct {
    const Self = @This();

    message_id: u64 = 0,
    transaction_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,

    pub fn packedSize(self: Self) usize {
        return Self.minimumSize() + self.topic_name_length;
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + @sizeOf(u64) + 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.message_id, .big);
        i += @sizeOf(u64);

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.transaction_id, .big);
        i += @sizeOf(u64);

        buf[i] = self.topic_name_length;
        i += 1;

        @memcpy(buf[i .. i + self.topic_name_length], self.topic_name[0..self.topic_name_length]);
        i += @intCast(self.topic_name_length);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const message_id = std.mem.readInt(u64, data[i..@sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const transaction_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const topic_name_length = data[i];
        i += 1;

        if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
        if (i + topic_name_length > data.len) return error.Truncated;

        var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
        @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);

        return Self{
            .message_id = message_id,
            .transaction_id = transaction_id,
            .topic_name_length = topic_name_length,
            .topic_name = topic_name,
        };
    }
};

test "size of structs" {
    try testing.expectEqual(8, @sizeOf(FixedHeaders));
    try testing.expectEqual(6, FixedHeaders.packedSize());

    const message = Message.new(.undefined);
    try testing.expectEqual(16, message.size());
    try testing.expectEqual(14, message.packedSize());
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
        .subscribe,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        const bytes = message.serialize(&buf);

        try testing.expect(bytes == message.packedSize());

        switch (message_type) {
            .undefined => try testing.expectEqual(bytes, 14),
            .publish => try testing.expectEqual(bytes, 23),
            .subscribe => try testing.expectEqual(bytes, 31),
        }
    }
}

test "message deserialization" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
        .subscribe,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        // serialize the message
        const bytes = message.serialize(&buf);

        // deserialize the message
        var deserialized_message = try Message.deserialize(buf[0..bytes]);

        try testing.expectEqual(message.size(), deserialized_message.size());
        try testing.expectEqual(message.packedSize(), deserialized_message.packedSize());
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
            .subscribe => {
                try testing.expectEqual(
                    message.extension_headers.subscribe.message_id,
                    deserialized_message.extension_headers.subscribe.message_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.transaction_id,
                    deserialized_message.extension_headers.subscribe.transaction_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.topic_name_length,
                    deserialized_message.extension_headers.subscribe.topic_name_length,
                );
                try testing.expect(std.mem.eql(u8, message.topicName(), deserialized_message.topicName()));
            },
            else => {},
        }
    }
}

test "message deserialization2" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
        .subscribe,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        // serialize the message
        const bytes = message.serialize(&buf);

        // deserialize the message
        const deserialized_result = try Message.deserialize2(buf[0..bytes]);

        try testing.expectEqual(bytes, deserialized_result.bytes_consumed);

        var deserialized_message = deserialized_result.message;

        try testing.expectEqual(message.size(), deserialized_message.size());
        try testing.expectEqual(message.packedSize(), deserialized_message.packedSize());
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
            .subscribe => {
                try testing.expectEqual(
                    message.extension_headers.subscribe.message_id,
                    deserialized_message.extension_headers.subscribe.message_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.transaction_id,
                    deserialized_message.extension_headers.subscribe.transaction_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.topic_name_length,
                    deserialized_message.extension_headers.subscribe.topic_name_length,
                );
                try testing.expect(std.mem.eql(u8, message.topicName(), deserialized_message.topicName()));
            },
            else => {},
        }
    }
}
