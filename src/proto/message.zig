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
    // Enqueue,
    // Deque,
    // Peek,
    // Put,
    // Get,
    // Delete,
    // Forward,
    // Broadcast,
};

// TODO: This is the interface I want
// try Message.serialize(&msg, &buf);
// const msg = Message.deserialize(&buf);

pub const Message = struct {
    const Self = @This();

    id: []const u8,
    // headers: Headers,
    message_type: u8,
    tx_id: []const u8,
    topic: []const u8,
    content: []const u8,

    pub fn new() Message {
        return Message{
            .id = "id",
            .tx_id = "",
            .message_type = @intFromEnum(MessageType.Undefined),
            // .headers = Headers.new(),
            .topic = "",
            .content = &.{},
        };
    }

    pub fn toRequest(self: *Self, topic: []const u8, content: []const u8) void {
        self.message_type = @intFromEnum(MessageType.Request);
        self.tx_id = "tx_id";
        self.topic = topic;
        self.content = content;
    }

    pub fn stringify(self: *Self, writer: anytype, options: cbor.Options) !void {
        try cbor.stringify(self, options, writer);
    }

    pub fn cborParse(data_item: cbor.DataItem, options: cbor.Options) !Self {
        return try cbor.parse(Self, data_item, options);
    }
};

// test "serialize a Message to cbor" {
//     var bytes = std.ArrayList(u8).init(std.testing.allocator);
//     defer bytes.deinit();
//
//     var msg = Message.new();
//     try msg.stringify(bytes.writer(), .{});
//
//     const want = [_]u8{ 166, 98, 105, 100, 66, 105, 100, 103, 104, 101, 97, 100, 101, 114, 115, 160, 108, 109, 101, 115, 115, 97, 103, 101, 95, 116, 121, 112, 101, 0, 101, 116, 120, 95, 105, 100, 64, 101, 116, 111, 112, 105, 99, 64, 103, 99, 111, 110, 116, 101, 110, 116, 64 };
//
//     // std.debug.print("want {any}\n", .{want});
//     // std.debug.print("got {any}\n", .{bytes.items});
//
//     try std.testing.expect(std.mem.eql(u8, &want, bytes.items));
// }

test "deserializes cbor to a Message" {
    var bytes = std.ArrayList(u8).init(std.testing.allocator);
    defer bytes.deinit();

    var msg = Message.new();
    msg.topic = "/hello";
    try msg.stringify(bytes.writer(), .{});

    std.debug.print("got {any}\n", .{bytes.items});

    const di = try cbor.DataItem.new(bytes.items);
    const got = try cbor.parse(Message, di, .{ .allocator = std.testing.allocator });

    std.debug.print("got {any}\n", .{got});
}
