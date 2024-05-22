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
