const std = @import("std");

pub const MessageParser = struct {
    const Self = @This();
    buffer: std.ArrayList(u8),
    // messages: std.ArrayList([]u8),

    pub fn init(allocator: std.mem.Allocator) MessageParser {
        return MessageParser{
            .buffer = std.ArrayList(u8).init(allocator),
            // .messages = std.ArrayList([]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
        // self.messages.deinit();
    }

    // i'm a dummy, this needs to be a pointer to self because we are modifying the struct!
    pub fn parse(self: *Self, messages: *std.ArrayList([]u8), data: []const u8) !void {
        // Append incoming data to the buffer
        try self.buffer.appendSlice(data);

        while (self.buffer.items.len >= 4) {
            // Read the length prefix
            var bytes: [4]u8 = undefined;
            // Read the length prefix
            const slice = self.buffer.items[0..4];
            // convert the slice into a 4 byte array
            // i'm sure there is a better way to do this
            for (slice, 0..4) |b, i| {
                bytes[i] = b;
            }

            const message_length = beToU32(bytes);

            // Check if the buffer contains the complete message
            if (self.buffer.items.len >= message_length + 4) {
                // Slice the buffer to extract message content
                const message = self.buffer.items[4 .. 4 + message_length];
                try messages.append(message);

                self.buffer.items = self.buffer.items[4 + message_length ..];
            } else {
                // Incomplete message in the buffer, wait for more data
                break;
            }
        }

        // return self.messages.clone();
        // return messages.toOwnedSlice();
        return;
    }
};

pub fn beToU32(bytes: [4]u8) u32 {
    return std.mem.readInt(u32, &bytes, .big);
}

test "convert big endian bytes to u32" {
    // setup
    const bytes = [4]u8{ 0, 0, 0, 5 };
    const want: u32 = 5;

    const got = beToU32(bytes);

    try std.testing.expectEqual(want, got);
}

test "parses length prefixed messages" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var al = std.ArrayList([]u8).init(allocator);
    try std.testing.expectEqual(0, al.items.len);

    const raw_bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    const want_1 = [_]u8{ 104, 101, 108, 108, 111 };
    const want_2 = [_]u8{ 104, 101, 108, 108, 111, 111, 111 };

    var parser = MessageParser.init(allocator);
    defer parser.deinit();

    try parser.parse(&al, &raw_bytes);

    try std.testing.expectEqual(2, al.items.len);
    try std.testing.expect(std.mem.eql(u8, &want_1, al.items[0]));
    try std.testing.expect(std.mem.eql(u8, &want_2, al.items[1]));
}

//
// // 05hello07hellooo
// // 05hello
// // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
// var timer = try std.time.Timer.start();
//
// const iters = 1_000_000;
// std.debug.print("sending {} bytes through parser {} times\n", .{ bytes.len, iters });
// for (0..iters) |_| {
//     _ = try parser.parse(&bytes);
// }
//
// const took = timer.read();
// std.debug.print("took {}ns\n", .{took});
// std.debug.print("took {}us\n", .{took / std.time.ns_per_us});
// std.debug.print("took {}ms\n", .{took / std.time.ns_per_ms});
