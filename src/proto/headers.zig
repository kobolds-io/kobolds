const std = @import("std");
const cbor = @import("zbor");

pub const Headers = struct {
    const Self = @This();

    token: ?[]const u8,

    pub fn new(token: ?[]const u8) Self {
        return Headers{
            .token = token,
        };
    }

    pub fn create(allocator: std.mem.Allocator, token: ?[]const u8) !*Self {
        const ptr = try allocator.create(Headers);
        ptr.* = Headers.new(token);

        return ptr;
    }

    pub fn cborStringify(self: Self, o: cbor.Options, out: anytype) !void {
        try cbor.stringify(self, .{
            .from_callback = true,
            .field_settings = &.{
                .{
                    .name = "token",
                    .field_options = .{ .alias = "0", .serialization_type = .TextString },
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
                    .name = "token",
                    .field_options = .{ .alias = "0", .serialization_type = .TextString },
                },
            },
            .allocator = o.allocator,
        });
    }
};
