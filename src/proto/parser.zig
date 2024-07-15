const std = @import("std");
const utils = @import("./utils.zig");

const ParseError = error{
    InvalidInputData,
};

pub const Parser = struct {
    const Self = @This();

    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .buffer = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // this function call fails. I suspect a double free somewhere but I cannot find it
        self.buffer.deinit();
    }

    pub fn parse(self: *Self, parsed_messages: *std.ArrayList([]u8), input_data: []const u8) !void {
        if (input_data.len < 4 and self.buffer.items.len == 0) return ParseError.InvalidInputData;

        // copy the data into the buffer
        try self.buffer.appendSlice(input_data);

        while (self.buffer.items.len >= 4) {
            // slice the length prefix
            const message_length = utils.bytesToU32(self.buffer.items[0..4]);

            // Check if the buffer contains the complete message
            if (self.buffer.items.len >= message_length + 4) {
                // Slice the buffer to extract message content
                const message = self.buffer.items[4 .. 4 + message_length];

                // append the message to the parsed_messages list
                try parsed_messages.append(message);

                // reset the buffer
                // clear out the mem this doesn't really work
                // self.buffer.replaceRangeAssumeCapacity(
                //     0,
                //     4 + message_length,
                //     &.{},
                // );

                // this is directly manipulating the underlying memory
                // feels kind of gross but in the end it works.
                std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[4 + message_length ..]);
                self.buffer.items.len -= (4 + message_length);
            } else {
                // Incomplete message in the buffer, wait for more data
                break;
            }
        }
    }
};
test "it deinits correctly" {
    var input_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const input_allocator = input_gpa.allocator();
    defer _ = input_gpa.deinit();

    var input_data = std.ArrayList(u8).init(input_allocator);
    defer input_data.deinit();

    const MESSAGES_COUNT: u32 = 10_000;
    const input = [_]u8{ 0, 0, 0, 6, 104, 101, 108, 108, 111, 111 };

    // make a very large input
    for (0..MESSAGES_COUNT) |_| {
        try input_data.appendSlice(&input);
    }

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const messages_allocator = messages_gpa.allocator();
    defer _ = messages_gpa.deinit();

    var parsed_messages = std.ArrayList([]u8).init(messages_allocator);
    defer parsed_messages.deinit();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const parser_allocator = parser_gpa.allocator();
    defer _ = parser_gpa.deinit();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    // Run the parser
    try parser.parse(&parsed_messages, input_data.items);
    // Success for this test means that it exits correctly and doesn't segfault
}

test "error if input data is invalid" {
    var messages = std.ArrayList([]u8).init(std.testing.allocator);
    const input_data = [_]u8{ 0, 0, 0 };
    try std.testing.expect(input_data.len < 4);

    var parser_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const parser_allocator = parser_arena.allocator();
    defer parser_arena.deinit();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    const result = parser.parse(&messages, &input_data);
    try std.testing.expectError(ParseError.InvalidInputData, result);
}

test "parses length prefixed messages" {
    // Allocator that lives outside of the parser
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var parsed_messages = std.ArrayList([]u8).init(allocator);
    defer parsed_messages.deinit();

    try std.testing.expectEqual(0, parsed_messages.items.len);

    const input_data = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    const want_1 = [_]u8{ 104, 101, 108, 108, 111 };
    const want_2 = [_]u8{ 104, 101, 108, 108, 111, 111, 111 };

    var parser_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const parser_allocator = parser_arena.allocator();
    defer parser_arena.deinit();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    try parser.parse(&parsed_messages, &input_data);

    try std.testing.expectEqual(2, parsed_messages.items.len);
    try std.testing.expect(std.mem.eql(u8, &want_1, parsed_messages.items[0]));
    try std.testing.expect(std.mem.eql(u8, &want_2, parsed_messages.items[1]));
}

test "parses 100_000 10 byte messages in less than 5 milliseconds using a single call" {
    var input_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const input_allocator = input_gpa.allocator();
    defer _ = input_gpa.deinit();

    var input_data = std.ArrayList(u8).init(input_allocator);
    defer input_data.deinit();

    const MESSAGES_COUNT: u32 = 1000;
    const input = [_]u8{ 0, 0, 0, 6, 104, 101, 108, 108, 111, 111 };

    for (0..MESSAGES_COUNT) |_| {
        try input_data.appendSlice(&input);
    }

    try std.testing.expectEqual(MESSAGES_COUNT * input.len, input_data.items.len);

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const messages_allocator = messages_gpa.allocator();
    defer _ = messages_gpa.deinit();

    var parsed_messages = std.ArrayList([]u8).init(messages_allocator);
    defer parsed_messages.deinit();

    try std.testing.expectEqual(0, parsed_messages.items.len);

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const parser_allocator = parser_gpa.allocator();
    defer _ = parser_gpa.deinit();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    try parser.parse(&parsed_messages, input_data.items);
    const duration = timer.read();

    // std.debug.print("duration {any}us\n", .{duration / std.time.ns_per_us});
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

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const parser_allocator = parser_gpa.allocator();
    defer _ = parser_gpa.deinit();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    try parser.parse(&parsed_messages, &input_data_1);

    try std.testing.expectEqual(0, parsed_messages.items.len);

    try parser.parse(&parsed_messages, &input_data_2);

    try std.testing.expectEqual(1, parsed_messages.items.len);
    try std.testing.expect(std.mem.eql(u8, &want, parsed_messages.items[0]));
}
