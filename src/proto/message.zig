const std = @import("std");
const cbor = @import("zbor");
const utils = @import("./utils.zig");

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

pub const Message = struct {
    const Self = @This();

    // id: u8,
    id: ?[]const u8,
    message_type: u8,
    content: ?[]const u8,
    tx_id: ?[]const u8,
    topic: ?[]const u8,
    // headers: Headers,
    // allocator: std.mem.Allocator,

    // return a stack Message
    pub fn new(id: []const u8, topic: []const u8, content: []const u8) Message {
        return Message{
            .id = id,
            .message_type = @intFromEnum(MessageType.Undefined),
            .topic = topic,
            .content = content,
            .tx_id = null,
            // .headers = Headers.new(),
        };
    }

    // return a heap Message
    pub fn create(allocator: std.mem.Allocator, id: []const u8, topic: []const u8, content: []const u8) !*Message {
        const msg_ptr = try allocator.create(Message);
        msg_ptr.* = Message.new(id, topic, content);

        return msg_ptr;
    }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString } },
                .{ .name = "message_type", .field_options = .{ .alias = "1", .serialization_type = .Integer } },
                .{ .name = "topic", .field_options = .{ .alias = "2", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "content", .field_options = .{ .alias = "3", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "tx_id", .field_options = .{ .alias = "4", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
            },
            .allocator = o.allocator,
        }, out);
    }

    pub fn cborParse(item: cbor.DataItem, o: cbor.Options) !Self {
        return try cbor.parse(Self, item, .{
            .from_callback = true, // prevent infinite loops
            .field_settings = &.{
                .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString } },
                .{ .name = "message_type", .field_options = .{ .alias = "1", .serialization_type = .Integer } },
                .{ .name = "topic", .field_options = .{ .alias = "2", .serialization_type = .TextString } },
                .{ .name = "content", .field_options = .{ .alias = "3", .serialization_type = .TextString } },
                .{ .name = "tx_id", .field_options = .{ .alias = "4", .serialization_type = .TextString } },
            },
            .allocator = o.allocator,
        });
    }
};

test "deserializes cbor to a Message" {
    var msg_on_stack = Message.new("stack_id", "hello", "there");

    // serialize the message
    const allocator = std.testing.allocator;
    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    try msg_on_stack.cborStringify(.{}, bytes.writer());

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const al = gpa.allocator();
    // defer _ = gpa.deinit();

    const di: cbor.DataItem = try cbor.DataItem.new(bytes.items);
    const parsed_msg = try Message.cborParse(di, .{ .allocator = al });

    try std.testing.expect(std.mem.eql(u8, msg_on_stack.id.?, parsed_msg.id.?));
    try std.testing.expectEqual(msg_on_stack.message_type, parsed_msg.message_type);
    try std.testing.expect(std.mem.eql(u8, msg_on_stack.topic.?, parsed_msg.topic.?));
    try std.testing.expect(std.mem.eql(u8, msg_on_stack.content.?, parsed_msg.content.?));

    // Message.tx_id defaults to null
    try std.testing.expectEqual(msg_on_stack.tx_id, parsed_msg.tx_id);
}
