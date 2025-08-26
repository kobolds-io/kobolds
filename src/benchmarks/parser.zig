const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Parser = @import("../protocol/parser.zig").Parser;
const Message = @import("../protocol/message.zig").Message;

const constants = @import("../constants.zig");

const ParserParseBenchmark = struct {
    messages: *std.array_list.Managed(Message),
    parser: *Parser,
    bytes: []const u8,

    fn new(messages: *std.array_list.Managed(Message), parser: *Parser, bytes: []const u8) ParserParseBenchmark {
        return .{
            .messages = messages,
            .parser = parser,
            .bytes = bytes,
        };
    }

    pub fn run(self: ParserParseBenchmark, _: std.mem.Allocator) void {
        self.parser.parse(self.messages, self.bytes) catch unreachable;
    }
};

fn afterEach() void {
    // reset the messages array list so we don't eat through all the memory on the machine
    // drop the len completely
    parser_messages.items.len = 0;
}

var parser_messages: std.array_list.Managed(Message) = undefined;

test "Parser benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var parser_messages_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = parser_messages_gpa.deinit();
    const parser_messages_allocator = parser_messages_gpa.allocator();

    parser_messages = std.array_list.Managed(Message).initCapacity(parser_messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer parser_messages.deinit();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    const headers_1 = [_]u8{ 185, 109, 74, 197, 189, 38, 87, 150, 98, 99, 131, 40, 248, 184, 217, 148, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100 };

    const body = [_]u8{97} ** constants.message_max_body_size;
    const bytes_1 = headers_1 ++ body;
    const bytes_2 = bytes_1 ++ headers_1 ++ body;
    const bytes_3 = bytes_2 ++ headers_1 ++ body;

    const recv_buffer = try parser_messages_allocator.alloc(u8, constants.connection_recv_buffer_size);
    defer parser_messages_allocator.free(recv_buffer);

    // write as many messages as possible into the recv_buffer. This is the closest represenation of what the parser
    // would have to deal with for each connection
    var recv_buffer_index: usize = 0;
    while (recv_buffer[recv_buffer_index..].len >= bytes_1.len) {
        @memcpy(recv_buffer[recv_buffer_index .. recv_buffer_index + bytes_1.len], &bytes_1);
        recv_buffer_index += bytes_1.len;
    }

    const parser_parse_1_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_1.len},
    );
    defer parser_messages_allocator.free(parser_parse_1_title);

    const parser_parse_2_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_2.len},
    );
    defer parser_messages_allocator.free(parser_parse_2_title);

    const parser_parse_3_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_3.len},
    );
    defer parser_messages_allocator.free(parser_parse_3_title);

    // create a single encoded message with no body
    var message_2 = Message.new();
    message_2.headers.message_type = .ping;
    message_2.setTransactionId(1);

    const bytes_4 = try parser_messages_allocator.alloc(u8, message_2.size());
    defer parser_messages_allocator.free(bytes_4);
    message_2.encode(bytes_4);

    const parser_parse_4_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{bytes_4.len},
    );
    defer parser_messages_allocator.free(parser_parse_4_title);

    const parser_parse_5_title = try std.fmt.allocPrint(
        parser_messages_allocator,
        "parse {} bytes",
        .{recv_buffer[0..recv_buffer_index].len},
    );
    defer parser_messages_allocator.free(parser_parse_5_title);

    try bench.addParam(
        parser_parse_4_title,
        &ParserParseBenchmark.new(&parser_messages, &parser, bytes_4),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_1_title,
        &ParserParseBenchmark.new(&parser_messages, &parser, &bytes_1),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_2_title,
        &ParserParseBenchmark.new(&parser_messages, &parser, &bytes_2),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_3_title,
        &ParserParseBenchmark.new(&parser_messages, &parser, &bytes_3),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_5_title,
        &ParserParseBenchmark.new(&parser_messages, &parser, recv_buffer[0..recv_buffer_index]),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );

    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|-------------------|\n");
    try stderr.writeAll("| Parser Benchmarks |\n");
    try stderr.writeAll("|-------------------|\n");
    try bench.run(stderr);
}
