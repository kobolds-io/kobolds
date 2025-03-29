const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Parser = @import("../protocol/parser.zig").Parser;
const Message = @import("../protocol/message.zig").Message;

const constants = @import("../constants.zig");

const ParserBenchmark = struct {
    messages: *std.ArrayList(Message),
    parser: *Parser,
    bytes: []const u8,

    fn new(messages: *std.ArrayList(Message), parser: *Parser, bytes: []const u8) ParserBenchmark {
        return .{
            .messages = messages,
            .parser = parser,
            .bytes = bytes,
        };
    }

    pub fn run(self: ParserBenchmark, _: std.mem.Allocator) void {
        self.parser.parse(self.messages, self.bytes) catch unreachable;
    }
};

test "Parser benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var parser_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser_messages_gpa.deinit();
    const parser_messages_allocator = parser_messages_gpa.allocator();

    var parser_messages = std.ArrayList(Message).initCapacity(parser_messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer parser_messages.deinit();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    const headers_1 = [_]u8{ 23, 215, 237, 54, 195, 69, 158, 226, 0, 0, 0, 0, 0, 0, 0, 0, 148, 217, 184, 248, 40, 131, 99, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    const body = [_]u8{97} ** constants.message_max_body_size;
    const bytes_1 = headers_1 ++ body;
    const bytes_2 = bytes_1 ++ headers_1 ++ body;
    const bytes_3 = bytes_2 ++ headers_1 ++ body;

    const parser_parse_1_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_1.len},
    );

    const parser_parse_2_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_2.len},
    );

    const parser_parse_3_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_3.len},
    );

    var message_2 = Message.new();
    message_2.headers.message_type = .ping;
    message_2.setTransactionId(1);

    const encoded_message_2 = try parser_messages_allocator.alloc(u8, message_2.size());
    message_2.encode(encoded_message_2);

    const bytes_4 = encoded_message_2;

    const parser_parse_4_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_4.len},
    );

    try bench.addParam(parser_parse_1_title, &ParserBenchmark.new(&parser_messages, &parser, &bytes_1), .{});
    try bench.addParam(parser_parse_2_title, &ParserBenchmark.new(&parser_messages, &parser, &bytes_2), .{});
    try bench.addParam(parser_parse_3_title, &ParserBenchmark.new(&parser_messages, &parser, &bytes_3), .{});
    try bench.addParam(parser_parse_4_title, &ParserBenchmark.new(&parser_messages, &parser, bytes_4), .{});

    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|-------------------|\n");
    try stderr.writeAll("| Parser Benchmarks |\n");
    try stderr.writeAll("|-------------------|\n");
    try bench.run(stderr);
}
