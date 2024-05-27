const std = @import("std");
const cbor = @import("zbor");

pub const Message = struct {
    const Self = @This();

    id: []const u8,
    topic: []const u8,
    content: []const u8,

    pub fn new(topic: []const u8, content: []const u8) Message {
        return Message{
            .id = "id",
            .topic = topic,
            .content = content,
        };
    }

    pub fn cborParse(item: cbor.DataItem, options: cbor.Options) !Self {
        return try cbor.parse(Self, item, options);
    }

    pub fn cborStringify(self: *const Self, options: cbor.Options, writer: anytype) !void {
        try cbor.stringify(self, options, writer);
    }
};

test "serializes to cbor" {
    var di = std.ArrayList(u8).init(std.testing.allocator);
    defer di.deinit();

    const content = &.{ 1, 2, 3, 4, 5 };
    const msg = Message.new("/hello", content);

    // std.debug.print("message {any}\n", .{msg});

    // try std.testing.expectEqual()
    const want = [_]u8{ 163, 98, 105, 100, 66, 105, 100, 101, 116, 111, 112, 105, 99, 70, 47, 104, 101, 108, 108, 111, 103, 99, 111, 110, 116, 101, 110, 116, 69, 1, 2, 3, 4, 5 };

    try msg.cborStringify(.{}, di.writer());
    // std.debug.print("cbor stringify output {d}\n", .{di.items});
    // try std.testing.expectEqual("asdfasdf", di.items);
    try std.testing.expect(std.mem.eql(u8, &want, di.items));
}
