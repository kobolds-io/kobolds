const std = @import("std");
const utils = @import("./utils.zig");

const ParseError = error{
    InvalidInputData,
    NotEnoughCapacity,
};

pub const MessageParser = struct {
    const Self = @This();

    buffer: std.ArrayList(u8) = undefined,

    pub fn init(buffer_allocator: std.mem.Allocator) MessageParser {
        return MessageParser{
            .buffer = std.ArrayList(u8).init(buffer_allocator),
        };
    }

    // idk if i should deinit this or not. i think so
    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    pub fn parse(self: *Self, parsed_messages: *std.ArrayList([]u8), input_data: []const u8) !void {
        if (input_data.len < 4 and self.buffer.items.len == 0) return ParseError.InvalidInputData;

        // copy the data into the buffer
        try self.buffer.appendSlice(input_data);

        while (self.buffer.items.len >= 4) {
            // slice the length prefix
            const message_length = utils.beToU32(self.buffer.items[0..4]);

            // Check if the buffer contains the complete message
            if (self.buffer.items.len >= message_length + 4) {
                // Slice the buffer to extract message content
                const message = self.buffer.items[4 .. 4 + message_length];

                // append the message to the parsed_messages list
                try parsed_messages.append(message);

                // reset the buffer
                self.buffer.items = self.buffer.items[4 + message_length ..];
            } else {
                // Incomplete message in the buffer, wait for more data
                break;
            }
        }
    }
};

test "error if input data is invalid" {
    var messages = std.ArrayList([]u8).init(std.testing.allocator);
    const input_data = [_]u8{ 0, 0, 0 };
    try std.testing.expect(input_data.len < 4);

    var parser = MessageParser.init(std.testing.allocator);
    defer parser.deinit();

    const result = parser.parse(&messages, &input_data);
    try std.testing.expectError(ParseError.InvalidInputData, result);
}

test "parses length prefixed messages" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var parsed_messages = std.ArrayList([]u8).init(allocator);
    try std.testing.expectEqual(0, parsed_messages.items.len);

    const input_data = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    const want_1 = [_]u8{ 104, 101, 108, 108, 111 };
    const want_2 = [_]u8{ 104, 101, 108, 108, 111, 111, 111 };

    var parser = MessageParser.init(std.testing.allocator);
    defer parser.deinit();

    try parser.parse(&parsed_messages, &input_data);

    try std.testing.expectEqual(2, parsed_messages.items.len);
    try std.testing.expect(std.mem.eql(u8, &want_1, parsed_messages.items[0]));
    try std.testing.expect(std.mem.eql(u8, &want_2, parsed_messages.items[1]));
}

test "parses 100_000 10 byte messages in less than 5 milliseconds using a single call" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var parsed_messages = std.ArrayList([]u8).init(allocator);

    try std.testing.expectEqual(0, parsed_messages.items.len);

    const MESSAGES_COUNT: u32 = 100_000;
    const input_data = comptime [_]u8{ 0, 0, 0, 6, 104, 101, 108, 108, 111, 111 } ** MESSAGES_COUNT;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const buffer_allocator = arena.allocator();

    // Initialize the parser here
    var parser = MessageParser.init(buffer_allocator);

    var timer = try std.time.Timer.start();
    defer timer.reset();

    try parser.parse(&parsed_messages, &input_data);
    const duration = timer.read();

    try std.testing.expect((duration / std.time.ns_per_ms) <= 5);

    try std.testing.expectEqual(MESSAGES_COUNT, parsed_messages.items.len);

    const want = [_]u8{ 104, 101, 108, 108, 111, 111 };
    for (parsed_messages.items) |m| {
        // ensure that every message is what we expect
        try std.testing.expect(std.mem.eql(u8, &want, m));
    }
}

test "parses incomplete messages with multiple parse calls" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var parsed_messages = std.ArrayList([]u8).init(allocator);
    try std.testing.expectEqual(0, parsed_messages.items.len);

    const input_data_1 = [_]u8{ 0, 0, 0, 5, 104, 101, 108 };
    const input_data_2 = [_]u8{ 108, 111 };
    const want = [_]u8{ 104, 101, 108, 108, 111 };

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const buffer_allocator = arena.allocator();

    var parser = MessageParser.init(buffer_allocator);
    // defer parser.deinit();

    try parser.parse(&parsed_messages, &input_data_1);
    try std.testing.expectEqual(0, parsed_messages.items.len);

    try parser.parse(&parsed_messages, &input_data_2);

    try std.testing.expectEqual(1, parsed_messages.items.len);
    try std.testing.expect(std.mem.eql(u8, &want, parsed_messages.items[0]));
}
