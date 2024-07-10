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

// pub const Meta = struct {
//     gen: []u8,
//     name: []u8,
//     times: Times,
//     allocator: std.mem.Allocator,
//
//     pub fn new(gen: []const u8, name: []const u8, allocator: std.mem.Allocator, milliTimestamp: *const fn () i64) !@This() {
//         const gen_ = try allocator.dupe(u8, gen);
//         errdefer allocator.free(gen_);
//         const name_ = try allocator.dupe(u8, name);
//         const t = milliTimestamp();
//
//         return .{
//             .gen = gen_,
//             .name = name_,
//             .times = .{
//                 .creat = t,
//                 .mod = t,
//             },
//             .allocator = allocator,
//         };
//     }
//
//     pub fn deinit(self: *const @This()) void {
//         self.allocator.free(self.gen);
//         self.allocator.free(self.name);
//     }
//
//     pub fn updateTime(self: *@This(), milliTimestamp: *const fn () i64) void {
//         self.times.mod = milliTimestamp();
//     }
//
//     pub fn cborStringify(self: *const @This(), o: cbor.Options, out: anytype) !void {
//         try cbor.stringify(self, .{
//             .from_callback = true,
//             .field_settings = &.{
//                 .{ .name = "gen", .field_options = .{ .alias = "0", .serialization_type = .Integer }, .value_options = .{ .slice_serialization_type = .TextString  } },
//                 .{ .name = "name", .field_options = .{ .alias = "1", .serialization_type = .Integer }, .value_options = .{ .slice_serialization_type = .TextString } },
//                 .{ .name = "times", .field_options = .{ .alias = "2", .serialization_type = .Integer } },
//                 .{ .name = "allocator", .field_options = .{ .skip = .Skip } },
//             },
//             .allocator = o.allocator,
//         }, out);
//     }
//
//     pub fn cborParse(item: cbor.DataItem, o: cbor.Options) !@This() {
//         return try cbor.parse(@This(), item, .{
//             .from_callback = true, // prevent infinite loops
//             .field_settings = &.{
//                 .{ .name = "gen", .field_options = .{ .alias = "0", .serialization_type = .Integer }, .value_options = .{ .slice_serialization_type = .TextString } },
//                 .{ .name = "name", .field_options = .{ .alias = "1", .serialization_type = .Integer }, .value_options = .{ .slice_serialization_type = .TextString } },
//                 .{ .name = "times", .field_options = .{ .alias = "2", .serialization_type = .Integer } },
//                 .{ .name = "allocator", .field_options = .{ .skip = .Skip } },
//             },
//             .allocator = o.allocator,
//         });
//     }
// };

pub const Message = struct {
    const Self = @This();

    id: ?[]const u8,
    content: ?[]const u8,
    // tx_id: ?[]const u8,
    // topic: ?[]const u8,
    // headers: Headers,
    // message_type: u8,
    // allocator: std.mem.Allocator,

    // pub fn new() Message {
    //     return Message{
    //         .id = "",
    //         .content = "",
    //         .tx_id = "",
    //         .topic = "",
    //         .headers = Headers.new(),
    //         .message_type = @intFromEnum(MessageType.Undefined),
    //     };
    // }

    // return a stack Message
    pub fn new(id: []const u8, content: []const u8) Message {
        return Message{
            .id = id,
            .content = content,
        };
    }

    // return a heap Message
    pub fn create(allocator: std.mem.Allocator, id: []const u8, content: []const u8) !*Message {
        const msg_ptr = try allocator.create(Message);
        msg_ptr.* = Message.new(id, content);

        return msg_ptr;
    }

    // pub fn init(allocator: std.mem.Allocator) !Message {
    //     const id_ = try allocator.dupe(u8, "some id");
    //     errdefer allocator.free(id_);
    //     // const content_ = try allocator.dupe(u8, content);
    //     // errdefer allocator.free(content_);
    //
    //     return Message{
    //         .id = id_,
    //         .content = null,
    //         // .content = content_,
    //         .allocator = allocator,
    //     };
    // }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "content", .field_options = .{ .alias = "1", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "allocator", .field_options = .{ .skip = .Skip } },
            },
            .allocator = o.allocator,
        }, out);
    }

    pub fn cborParse(item: cbor.DataItem, o: cbor.Options) !@This() {
        return try cbor.parse(Self, item, .{
            .from_callback = true, // prevent infinite loops
            .field_settings = &.{
                .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "content", .field_options = .{ .alias = "1", .serialization_type = .TextString }, .value_options = .{ .slice_serialization_type = .TextString } },
                .{ .name = "allocator", .field_options = .{ .skip = .Skip } },
            },
            .allocator = o.allocator,
        });
    }
};

test "deserializes cbor to a Message" {
    // var node = try allocator.create(Node);
    // errdefer allocator.destroy(node);
    // node.* = .{ .next = self.head, .item = item, .prev = null };

    // const allocator = std.testing.allocator;
    // const msg_ptr = try allocator.create(Message);
    // defer allocator.destroy(msg_ptr);

    // var msg = msg_ptr.*;
    // msg = .{ .id = "hello", .content = null };

    // msg.content = "bat";

    // var msg = try Message.init(std.testing.allocator);
    // defer msg.deinit();
    // try msg.setContent("hello there");

    const allocator = std.testing.allocator;

    const msg_on_stack = Message.new("stack_id", "this is some cool content on the stack");
    const msg_on_heap = try Message.create(allocator, "stack_id", "this is some cool content on the heap");
    defer allocator.destroy(msg_on_heap);

    std.debug.print("msg_on_stack {any}\n", .{msg_on_stack});
    std.debug.print("msg_on_heap {any}\n", .{msg_on_heap.*});

    // msg.content = "fff";

    // std.debug.print("msg {any}\n", .{msg});
    // var bytes = std.ArrayList(u8).init(std.testing.allocator);
    // defer bytes.deinit();

    // var test_msg = Message.new();
    // test_msg.id = "asdf";
    // test_msg.content = "hello there!";

    // try cbor.stringify(test_msg, .{
    //     .field_settings = &.{
    //         .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString } },
    //         .{ .name = "content", .field_options = .{ .alias = "1", .serialization_type = .TextString } },
    //     },
    // }, bytes.writer());
    //
    // std.debug.print("bytes.items {s}\n", .{bytes.items});
    //
    // const di: cbor.DataItem = try cbor.DataItem.new(bytes.items);
    // const msg: Message = try cbor.parse(Message, di, .{ .allocator = std.testing.allocator, .field_settings = &.{
    //     .{ .name = "id", .field_options = .{ .alias = "0", .serialization_type = .TextString } },
    //     .{ .name = "content", .field_options = .{ .alias = "1", .serialization_type = .TextString } },
    // } });
    //
    // defer {
    //     if (msg.id != null) std.testing.allocator.free(msg.id.?);
    //     if (msg.content != null) std.testing.allocator.free(msg.content.?);
    // }
    //
    // std.debug.print("my msg {any}\n", .{msg});
    // std.debug.print("my msg.id {any}\n", .{msg.id});
    // std.debug.print("my msg.content {any}\n", .{msg.content});
}
