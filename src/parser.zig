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
        // initialize an ArrayList here to store the parsed messages
        // var messages = std.ArrayList([]u8).init(allocator);
        // at the end we drain the messages array list by
        // this shouldn't be needed as i'm calling toOwnedSlice
        // at the end of this func
        // defer messages.deinit();

        // Append incoming data to the buffer
        try self.buffer.appendSlice(data);

        var index: usize = 0;
        while (self.buffer.items.len - index >= 4) {
            // Read the length prefix
            // const message_length_bytes = self.buffer.items[index .. index + 4];
            var bytes: [4]u8 = undefined;
            const slice = self.buffer.items[index .. index + 4];
            // convert the slice into a 4 byte array
            for (slice, 0..4) |b, i| {
                bytes[i] = b;
            }

            const message_length = beToU32NoAllocator(bytes);

            // Check if the buffer contains the complete message
            if (self.buffer.items.len - index >= message_length + 4) {
                // Slice the buffer to extract message content
                const message = self.buffer.items[index + 4 .. index + 4 + message_length];
                try self.messages.append(message);

                // Move index past the current message
                index += 4 + message_length;
            } else {
                // Incomplete message in the buffer, wait for more data
                break;
            }
        }

        // Remove the parsed messages from the buffer
        if (index > 0) {
            self.buffer.items = self.buffer.items[index..];
        }
        // std.debug.print("messages len {}\n", .{messages.items.len});

        return self.messages.toOwnedSlice();
    }
};

pub fn beToU32NoAllocator(bytes: [4]u8) u32 {
    // if (bytes.len != 4) return ParseError.ReceivedInvalidBytes;
    // std.debug.print("bytes {any}\n", .{bytes});
    return std.mem.readInt(u32, &bytes, .big);
}

test "convert big endian bytes to u32" {
    // setup
    const bytes = [4]u8{ 0, 0, 0, 5 };
    const want: u32 = 5;

    const got = beToU32NoAllocator(bytes);

    try std.testing.expectEqual(want, got);
}
