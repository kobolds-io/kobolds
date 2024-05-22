const std = @import("std");

pub const MessageParser = struct {
    const Self = @This();
    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) MessageParser {
        return MessageParser{
            .buffer = std.ArrayList(u8).init(allocator),
        };
    }

    // i'm a dummy, this needs to be a pointer to self because we are modifying the struct!
    pub fn parse(self: *Self, allocator: std.mem.Allocator, data: []const u8) ![][]u8 {
        // std.debug.print("buffer capacity {any}\n", .{self.buffer.capacity});
        // initialize an ArrayList here to store the parsed messages
        var messages = std.ArrayList([]u8).init(allocator);
        // at the end we drain the messages array list by
        // this shouldn't be needed as i'm calling toOwnedSlice
        // at the end of this func
        defer messages.deinit();

        // Append incoming data to the buffer
        try self.buffer.appendSlice(data);

        var index: usize = 0;
        while (self.buffer.items.len - index >= 4) {
            // Read the length prefix
            const message_length_bytes = self.buffer.items[index .. index + 4];
            var stream = std.io.fixedBufferStream(message_length_bytes);
            const message_length = try beToU32(allocator, stream.reader());

            // Check if the buffer contains the complete message
            if (self.buffer.items.len - index >= message_length + 4) {
                // Slice the buffer to extract message content
                const message = self.buffer.items[index + 4 .. index + 4 + message_length];
                try messages.append(message);

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

        return messages.toOwnedSlice();
    }
};

pub const ParseError = error{ ReceivedInvalidBytes, CouldNotParseMessagePrefix };

// this is likely not very efficient but ermagerd it works
pub fn beToU32(allocator: std.mem.Allocator, reader: anytype) !u32 {
    var buffered_reader = std.io.bufferedReader(reader);
    var parsed_nums = std.ArrayList(u32).init(allocator);
    var parsed_number_buffer: [4]u8 = undefined;
    while (true) {
        const bytes_read = try buffered_reader.read(&parsed_number_buffer);
        if (bytes_read == 0) {
            break;
        }
        if (bytes_read != 4) {
            // Decide what to do if there's not exactly 4 bytes.
            // For now, we'll just error.
            return ParseError.ReceivedInvalidBytes;
        }
        const parsed_num = std.mem.readInt(u32, &parsed_number_buffer, .big);
        try parsed_nums.append(parsed_num);
    }

    // i don't know if we will ever hit this error
    if (parsed_nums.items.len != 1) {
        return ParseError.CouldNotParseMessagePrefix;
    }

    return parsed_nums.items[0];
}

test "convert big endian bytes to u32" {
    // setup
    const bytes = [_]u8{ 0, 0, 0, 5 };
    const bytes_mem = std.mem.sliceAsBytes(&bytes);
    var stream = std.io.fixedBufferStream(bytes_mem);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    // only deinit if something goes wrong with the test
    errdefer _ = gpa.deinit();

    const want: u32 = 5;
    const got = try beToU32(allocator, stream.reader());

    try std.testing.expectEqual(want, got);
}

test "convert big endian bytes returns error if invalid bytes" {
    // setup
    // the size of the byte array is larger than what the algorithm handles
    const bytes = [_]u8{ 0, 0, 0, 5, 0, 0 };
    const bytes_mem = std.mem.sliceAsBytes(&bytes);
    var stream = std.io.fixedBufferStream(bytes_mem);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    // only deinit if something goes wrong with the test
    errdefer _ = gpa.deinit();

    const got = beToU32(allocator, stream.reader());
    try std.testing.expectEqual(ParseError.ReceivedInvalidBytes, got);
}
