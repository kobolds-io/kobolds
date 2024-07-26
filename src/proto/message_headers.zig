const std = @import("std");
const cbor = @import("zbor");
const uuid = @import("uuid");

const MessageType = @import("./message_type.zig").MessageType;
const ErrorCode = @import("./error_code.zig").ErrorCode;

pub const MessageHeaders = struct {
    const Self = @This();

    error_code: ?ErrorCode,
    message_type: MessageType,
    token: ?[]const u8 = null,
    tx_id: ?[]const u8 = null,

    pub fn new() Self {
        return MessageHeaders{
            .error_code = null,
            .message_type = MessageType.Undefined,
            .token = null,
            .tx_id = null,
        };
    }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{
                    .name = "error_code",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .enum_serialization_type = .Integer },
                },
                .{
                    .name = "message_type",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .enum_serialization_type = .Integer },
                },
                .{
                    .name = "token",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "tx_id",
                    .field_options = .{ .alias = "3", .serialization_type = .Integer },
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
                    .name = "error_code",
                    .field_options = .{ .alias = "0", .serialization_type = .Integer },
                    .value_options = .{ .enum_serialization_type = .Integer },
                },
                .{
                    .name = "message_type",
                    .field_options = .{ .alias = "1", .serialization_type = .Integer },
                    .value_options = .{ .enum_serialization_type = .Integer },
                },
                .{
                    .name = "token",
                    .field_options = .{ .alias = "2", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
                .{
                    .name = "tx_id",
                    .field_options = .{ .alias = "3", .serialization_type = .Integer },
                    .value_options = .{ .slice_serialization_type = .TextString },
                },
            },
            .allocator = o.allocator,
        });
    }
};

test "MessageHeaders.cborParse" {
    // using an arena makes it easy for me to deinit everything all at once.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    // serialize the message
    var bytes = std.ArrayList(u8).init(allocator);
    defer bytes.deinit();

    var original = MessageHeaders.new();
    original.message_type = MessageType.Request;
    original.error_code = ErrorCode.ProtocolError;
    original.token = "my cool client token that totally is awesome";
    original.tx_id = &uuid.urn.serialize(uuid.v7.new());

    try original.cborStringify(.{}, bytes.writer());

    const di: cbor.DataItem = try cbor.DataItem.new(bytes.items);
    const parsed = try MessageHeaders.cborParse(di, .{ .allocator = allocator });

    try std.testing.expectEqual(original.message_type, parsed.message_type);
    try std.testing.expectEqual(original.error_code, parsed.error_code);
    try std.testing.expect(std.mem.eql(u8, original.token.?, parsed.token.?));
    try std.testing.expect(std.mem.eql(u8, original.tx_id.?, parsed.tx_id.?));
}
