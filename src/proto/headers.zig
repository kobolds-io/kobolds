const std = @import("std");
const cbor = @import("zbor");

pub const Headers = struct {
    const Self = @This();

    token: ?[]const u8,

    pub fn new() Self {
        return Headers{
            .token = null,
        };
    }

    pub fn stringify(self: *Self, writer: anytype, options: cbor.Options) !void {
        try cbor.stringify(self, options, writer);
    }

    pub fn cborParse(data_item: cbor.DataItem, options: cbor.Options) !Self {
        return try cbor.parse(Self, data_item, options);
    }
};
