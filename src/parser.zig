const std = @import("std");

// const MessageParser = struct {
//     buffer: std.ArrayList(u8),
//
//     pub fn init(allocator: *std.mem.Allocator) !MessageParser {
//         return MessageParser{
//             .buffer = try std.ArrayList(u8).init(allocator),
//         };
//     }
//
//     pub fn parse(self: *MessageParser, data: []const u8, allocator: *std.mem.Allocator) ![][]u8 {
//         var messages = try std.ArrayList([]u8).init(allocator);
//         defer messages.deinit();
//
//         // Append incoming data to the buffer
//         try self.buffer.appendSlice(data);
//
//         var index: usize = 0;
//         while (self.buffer.items.len - index >= 4) {
//             // Read the length prefix
//             const message_length_bytes = self.buffer.items[index .. index + 4];
//             const message_length = @intFromBytes(u32, message_length_bytes, .big);
//
//             // Check if the buffer contains the complete message
//             if (self.buffer.items.len - index >= @intCast(usize, message_length) + 4) {
//                 // Slice the buffer to extract message content
//                 const message = self.buffer.items[index + 4 .. index + 4 + @intCast(usize, message_length)];
//                 try messages.append(std.mem.dupe(u8, allocator, message));
//
//                 // Move index past the current message
//                 index += 4 + @intCast(usize, message_length);
//             } else {
//                 // Incomplete message in the buffer, wait for more data
//                 break;
//             }
//         }
//
//         // Remove the parsed messages from the buffer
//         if (index > 0) {
//             self.buffer.items = self.buffer.items[index..];
//         }
//
//         return messages.toOwnedSlice();
//     }
// };

// pub fn main() void {

// const allocator = std.heap.page_allocator;
// var parser = MessageParser.init(allocator) catch unreachable;

// Example usage
// 05hello
// const data: []const u8 = &[_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
// const i = @intFromPtr(&data[0..4]);
// std.debug.print("got this number! {}", .{i});

// const messages = parser.parse(data, allocator) catch unreachable;

// for (messages) |message| {
//     std.debug.print("Message: {s}\n", .{message});
// }
// }

pub const ParseError = error{ReceivedInvalidBytes};

pub fn readU32sFromReader(allocator: std.mem.Allocator, reader: anytype) !std.ArrayList(u32) {
    var buffered_reader = std.io.bufferedReader(reader);
    var numbers = std.ArrayList(u32).init(allocator);
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
        try numbers.append(parsed_num);
    }

    return numbers;
}

test "convert big endian bytes to an ArrayList of u32s" {
    // setup
    const bytes = [_]u8{ 0, 0, 0, 5 };
    const bytes_mem = std.mem.sliceAsBytes(&bytes);
    var stream = std.io.fixedBufferStream(bytes_mem);

    const want: u32 = 5;
    const got = try readU32sFromReader(std.testing.allocator, stream.reader());
    defer got.deinit();

    try std.testing.expectEqual(got.items.len, 1);
    try std.testing.expectEqual(want, got.items[0]);
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

    const got = readU32sFromReader(allocator, stream.reader());
    try std.testing.expectEqual(ParseError.ReceivedInvalidBytes, got);
}
