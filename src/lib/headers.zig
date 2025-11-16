const std = @import("std");
const assert = std.debug.assert;

pub const PingHeaders = struct {
    const Self = @This();

    transaction_id: u64 = 0,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(u64);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());

        var i: usize = 0;

        std.mem.writeInt(u64, buf[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], self.transaction_id, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.minimumSize()) return error.Truncated;

        var i: usize = 0;
        const transaction_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        return Self{
            .transaction_id = transaction_id,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.transaction_id == 0) return "invalid transaction_id";

        return null;
    }
};

pub const PongHeaders = struct {
    const Self = @This();

    transaction_id: u64 = 0,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(u64);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());

        var i: usize = 0;

        std.mem.writeInt(u64, buf[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], self.transaction_id, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.minimumSize()) return error.Truncated;

        var i: usize = 0;
        const transaction_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        return Self{
            .transaction_id = transaction_id,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.transaction_id == 0) return "invalid transaction_id";

        return null;
    }
};
