const std = @import("std");
const cbor = @import("zbor");
const uuid = @import("uuid");

const ErrorCode = @import("./error_code.zig").ErrorCode;
const MessageHeaders = @import("./message_headers.zig").MessageHeaders;
const MessageType = @import("./message_type.zig").MessageType;

pub const Message = struct {
    const Self = @This();

    id: [36]u8, // Urn
    topic: []const u8 = undefined,
    content: []const u8 = undefined,
    headers: MessageHeaders,

    pub fn new(topic: []const u8, content: []const u8) Message {
        return Message{
            // unique identifier of this message
            .id = uuid.urn.serialize(uuid.v7.new()),
            // message content bytes
            .content = content,
            // struct containing metadata about the message
            .headers = MessageHeaders.new(),
            // route for the message to take
            .topic = topic,
        };
    }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{
                    .name = "id",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .ByteString },
                },
                .{
                    .name = "content",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "headers",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "topic",
                    .field_options = .{ .alias = "4", .serialization_type = .Integer },
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
                    .value_options = .{ .slice_serialization_type = .ByteString },
                },
                .{
                    .name = "content",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "headers",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "topic",
                    .field_options = .{ .alias = "4", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
            },
            .allocator = o.allocator,
        });
    }
};

test "Message.cborParse" {
    var original_msg = Message.new("/hello", "world");

    // serialize the message
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    try original_msg.cborStringify(.{}, bytes.writer());

    const di: cbor.DataItem = try cbor.DataItem.new(bytes.items);
    const parsed_msg = try Message.cborParse(di, .{ .allocator = allocator });

    try std.testing.expect(std.mem.eql(u8, &original_msg.id, &parsed_msg.id));
    try std.testing.expectEqual(original_msg.id, parsed_msg.id);
    try std.testing.expect(std.mem.eql(u8, original_msg.topic, parsed_msg.topic));
    try std.testing.expect(std.mem.eql(u8, original_msg.content, parsed_msg.content));
    try std.testing.expectEqual(original_msg.headers, MessageHeaders.new());
    try std.testing.expectEqual(parsed_msg.headers, MessageHeaders.new());
}
