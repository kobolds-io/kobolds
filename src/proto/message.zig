const std = @import("std");
const cbor = @import("zbor");

const Headers = @import("./headers.zig").Headers;

pub const MessageType = enum(u8) {
    Undefined,
    Connect,
    KeepAlive,
    Register,
    Unregister,
    Request,
    Reply,
    Publish,
    Subscribe,
    Unsubscribe,
    Enqueue,
    Deque,
    Peek,
    Put,
    Get,
    Delete,
    Forward,
    Broadcast,
};

// Id          string      `cbor:"id"`
// MessageType MessageType `cbor:"message_type"`
// Topic       string      `cbor:"topic"`
// TxId        string      `cbor:"tx_id,omitempty"`
// Headers     Headers     `cbor:"headers,omitempty"`
// Content     []byte      `cbor:"content,omitempty"`
// Error       Error       `cbor:"error,omitempty"`

pub const MessageError = struct {
    const Self = @This();

    message: []const u8,
    code: u8,

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{
                    .name = "message",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "code",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                },
            },
            .allocator = o.allocator,
        }, out);
    }

    pub fn cborParse(item: cbor.DataItem, o: cbor.Options) !Self {
        return try cbor.parse(Self, item, .{
            .from_callback = true, // prevent infinite loops
            .field_settings = &.{
                .{
                    .name = "message",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "code",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                },
            },
            .allocator = o.allocator,
        });
    }
};

pub const Message = struct {
    const Self = @This();

    id: []const u8,
    message_type: u8,
    topic: []const u8,
    content: ?[]const u8,
    tx_id: ?[]const u8,
    headers: Headers,
    message_error: ?MessageError,

    // return a stack Message
    pub fn new(id: []const u8, topic: []const u8, content: []const u8) Message {
        return Message{
            .id = id,
            .topic = topic,
            .message_type = @intFromEnum(MessageType.Undefined),
            .content = content,
            .tx_id = null,
            .headers = Headers.new(null),
            .message_error = null,
        };
    }

    // return a heap Message
    pub fn create(allocator: std.mem.Allocator, id: []const u8, topic: []const u8, content: []const u8) !*Message {
        const ptr = try allocator.create(Message);
        ptr.* = Message.new(id, topic, content);

        return ptr;
    }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{
                    .name = "id",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "message_type",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .Integer },
                },
                .{
                    .name = "topic",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "tx_id",
                    .field_options = .{ .alias = "3", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "headers",
                    .field_options = .{ .alias = "4", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "content",
                    .field_options = .{ .alias = "5", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "message_error",
                    .field_options = .{ .alias = "6", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
            },
            .allocator = o.allocator,
        }, out);
    }

    pub fn cborParse(item: cbor.DataItem, o: cbor.Options) !Self {
        return try cbor.parse(Self, item, .{
            .from_callback = true, // prevent infinite loops
            .field_settings = &.{
                .{
                    .name = "id",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "message_type",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .Integer },
                },
                .{
                    .name = "topic",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "tx_id",
                    .field_options = .{ .alias = "3", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "headers",
                    .field_options = .{ .alias = "4", .serialization_type = .Integer },
                },
                .{
                    .name = "content",
                    .field_options = .{ .alias = "5", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "message_error",
                    .field_options = .{ .alias = "6", .serialization_type = .Integer },
                },
            },
            .allocator = o.allocator,
        });
    }
};

test "Message.cborParse" {
    var original_msg = Message.new("stack_id", "hello", "there");

    // serialize the message
    const allocator = std.testing.allocator;
    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    original_msg.headers.token = "my cool client token that totally is awesome";
    original_msg.message_error = MessageError{ .message = "some message", .code = 1 };
    try original_msg.cborStringify(.{}, bytes.writer());

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const al = gpa.allocator();
    // defer _ = gpa.deinit();

    const di: cbor.DataItem = try cbor.DataItem.new(bytes.items);
    const parsed_msg = try Message.cborParse(di, .{ .allocator = al });

    try std.testing.expect(std.mem.eql(u8, original_msg.id, parsed_msg.id));
    try std.testing.expectEqual(original_msg.message_type, parsed_msg.message_type);
    try std.testing.expect(std.mem.eql(u8, original_msg.topic, parsed_msg.topic));
    try std.testing.expect(std.mem.eql(u8, original_msg.content.?, parsed_msg.content.?));
    try std.testing.expect(std.mem.eql(u8, original_msg.headers.token.?, parsed_msg.headers.token.?));
    try std.testing.expect(std.mem.eql(u8, original_msg.message_error.?.message, parsed_msg.message_error.?.message));
    try std.testing.expectEqual(original_msg.message_error.?.code, parsed_msg.message_error.?.code);

    // Message.tx_id defaults to null
    try std.testing.expectEqual(original_msg.tx_id, parsed_msg.tx_id);
}
