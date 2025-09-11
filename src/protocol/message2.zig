const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const MAX_MESSAGE_SIZE = @sizeOf(FixedHeaders) + @sizeOf(ExtensionHeader) + constants.message_max_body_size;

pub const Message = struct {
    const Self = @This();
    fixed_headers: FixedHeaders = FixedHeaders{},
    extension: ExtensionHeader = ExtensionHeader{ .undefined = {} },
    body: [constants.message_max_body_size]u8 = undefined,

    pub fn new(message_type: MessageType) Self {
        return switch (message_type) {
            .undefined => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension = .{ .undefined = {} },
                .body = undefined,
            },
            .publish => Self{
                .fixed_headers = .{
                    .message_type = message_type,
                },
                .extension = .{ .publish = .{} },
                .body = undefined,
            },
        };
    }

    pub fn size(self: Self) usize {
        const extension_size: usize = switch (self.fixed_headers.message_type) {
            .undefined => 0,
            .publish => @sizeOf(PublishHeaders),
        };
        return @sizeOf(FixedHeaders) + extension_size + self.fixed_headers.body_length;
    }

    pub fn asBytes(self: *Self, allocator: std.mem.Allocator) ![]const u8 {
        _ = self;

        var list = std.ArrayList(u8).initCapacity(allocator, @sizeOf(Message)) catch unreachable;

        // list.appendAssumeCapacity(self.fixed_headers);

        return list.toOwnedSlice(allocator);

        // serialize the fixed headers
        // self.fixed_headers.asBytes(buf);

        // list.appendSliceAssumeCapacity(&utils.u64ToBytes(headers_checksum));
        // list.appendSliceAssumeCapacity(&utils.u64ToBytes(body_checksum));
        // list.appendSliceAssumeCapacity(&utils.u128ToBytes(self.origin_id));
        // list.appendSliceAssumeCapacity(&utils.u128ToBytes(self.connection_id));
        // list.appendSliceAssumeCapacity(&utils.u16ToBytes(self.body_length));
        // list.appendAssumeCapacity(@intFromEnum(self.protocol_version));
        // list.appendAssumeCapacity(@intFromEnum(self.message_type));
        // list.appendAssumeCapacity(@intFromEnum(self.compression));
        // list.appendAssumeCapacity(@intFromBool(self.compressed));
        // list.appendSliceAssumeCapacity(&self.padding);
        // list.appendSliceAssumeCapacity(&self.reserved);

        // assert(list.items.len == @sizeOf(Message));
        // return list.items;
    }
};

pub const FixedHeaders = packed struct {
    const Self = @This();

    body_length: u16 = 0,
    message_type: MessageType = .undefined,
    version: u4 = 0,
    flags: u4 = 0,
    extensions_count: u8 = 0,
    padding: u16 = 0,

    pub fn asBytes(self: *Self, buf: []u8) void {
        _ = self;
        _ = buf;
    }
};

pub const MessageType = enum(u8) {
    undefined,
    publish,
};

pub const ExtensionHeader = union(MessageType) {
    const Self = @This();
    undefined: void,
    publish: PublishHeaders,
};

pub const PublishHeaders = extern struct {
    message_id: u64 = 0,
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
    const allocator = testing.allocator;
    const message_types = [_]MessageType{
        .undefined,
        .publish,
    };

    for (message_types) |message_type| {
        var message = Message.new(message_type);

        const bytes = try message.asBytes(allocator);

        log.err("message: {any}", .{message});
        log.err("bytes: {any}", .{bytes});
    }
}
