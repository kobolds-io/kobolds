const std = @import("std");

pub const MessageParser = struct {
    const Self = @This();
    buffer: std.ArrayList(u8),
    messages: std.ArrayList([]u8),

    pub fn init(allocator: std.mem.Allocator) MessageParser {
        return MessageParser{
            .buffer = std.ArrayList(u8).init(allocator),
            .messages = std.ArrayList([]u8).init(allocator),
        };
    }

    // i'm a dummy, this needs to be a pointer to self because we are modifying the struct!
    pub fn parse(self: *Self, data: []const u8) ![][]u8 {
        // Append incoming data to the buffer
        std.debug.print("current buffer {any}\n", .{self.buffer.items});

        try self.buffer.appendSlice(data);

        while (self.buffer.items.len >= 4) {
            // Read the length prefix
            var bytes: [4]u8 = undefined;
            // Read the length prefix
            const slice = self.buffer.items[0..4];
            // convert the slice into a 4 byte array
            for (slice, 0..4) |b, i| {
                bytes[i] = b;
            }
            const message_length = beToU32(bytes);

            // Check if the buffer contains the complete message
            if (self.buffer.items.len >= message_length + 4) {
                // Slice the buffer to extract message content
                _ = self.buffer.items[4 .. 4 + message_length];
                // std.debug.print("message {any}\n", .{message});
                // try self.messages.append(message);
                // Move index past the current message
                // try self.buffer.resize(self.buffer.items.len - 4 + message_length);

                // I think the ArrayList is resizing under the hood without my knowledge
                // which fucks up all of the references
                std.debug.print("len {}\n", .{self.buffer.items.len});
                std.debug.print("capacity {}\n", .{self.buffer.capacity});
                // TODO: add a break point to figure out the state before and after this line

                self.buffer.items = self.buffer.items[4 + message_length ..];

                std.debug.print("len {}\n", .{self.buffer.items.len});
                std.debug.print("capacity {}\n", .{self.buffer.capacity});
            } else {
                // Incomplete message in the buffer, wait for more data
                break;
            }
        }

        return self.messages.toOwnedSlice();
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
