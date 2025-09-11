const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const MAX_MESSAGE_SIZE = @sizeOf(FixedHeaders) + @sizeOf(ExtensionHeaders) + constants.message_max_body_size;

pub const Message = struct {
    const Self = @This();
    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .undefined = {} },
    body_buffer: [constants.message_max_body_size]u8 = undefined,

    pub fn new(message_type: MessageType) Self {
        return switch (message_type) {
            .undefined => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension_headers = .{ .undefined = {} },
                .body_buffer = undefined,
            },
            .publish => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension_headers = .{ .publish = .{} },
                .body_buffer = undefined,
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

    pub fn size(self: Self) usize {
        const extension_size: usize = switch (self.fixed_headers.message_type) {
            .undefined => 0,
            .publish => @sizeOf(PublishHeaders),
        };
        return @sizeOf(FixedHeaders) + extension_size + self.fixed_headers.body_length;
    }

    pub fn asBytes(self: *Self, buf: []u8) usize {
        assert(buf.len == self.size());

        var i: usize = 0;

        const fixed_headers_bytes_len = self.fixed_headers.asBytes(buf[0..]);
        i += fixed_headers_bytes_len;

        const extension_headers_bytes_len = self.extension_headers.asBytes(buf[i..]);
        i += extension_headers_bytes_len;

        @memcpy(buf[i .. i + self.fixed_headers.body_length], self.body());
        i += self.fixed_headers.body_length;

        return i;
    }
};

pub const FixedHeaders = packed struct {
    const Self = @This();

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
    try testing.expectEqual(@sizeOf(FixedHeaders) + 0, undefined_message.size());

    var publish_message = Message.new(.publish);
    try testing.expectEqual(@sizeOf(FixedHeaders) + @sizeOf(PublishHeaders), publish_message.size());
}

test "message serializes to bytes" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(message_type);
        switch (message_type) {
            .publish => try testing.expectEqual(),
        }

        const bytes = message.asBytes(buf[0..message.size()]);
        log.err("\nmessage_type: {any}, bytes: {any}, len: {}", .{ message.fixed_headers.message_type, buf[0..bytes], bytes });
    }
}

pub fn writeStructBigEndian(comptime T: type, value: T, buf: []u8) usize {
    const info = @typeInfo(T);
    switch (info) {
        .@"struct" => |s| {
            var offset: usize = 0;
            inline for (s.fields) |field| {
                const F = field.type;
                const val = @field(value, field.name);

                if (@typeInfo(F) == .int) {
                    const size = @sizeOf(F);
                    assert(buf.len >= offset + size);

                    const ptr = buf[offset .. offset + size];
                    std.mem.writeInt(F, @ptrCast(ptr), val, .big);

                    offset += size;
                } else {
                    @compileError("Unsupported field type in writeStructBigEndian: " ++ field.name);
                }
            }
            return offset;
        },
        else => @compileError("writeStructBigEndian only works on structs"),
    }

    // return @as(usize, 0);
}
