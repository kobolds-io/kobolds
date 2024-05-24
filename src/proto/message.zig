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

test "stringify cbor" {
    var di = std.ArrayList(u8).init(std.testing.allocator);
    defer di.deinit();

    const c = [_]u8{ 1, 2, 3, 4, 5 };
    const content = c[0..];

    const msg = Message.new("/hello", content);
    std.debug.print("message {any}\n", .{msg});

    // try p.cborStringify(.{}, di.writer());
    // print("cbor stringify output {s}\n", .{di.items});
}
