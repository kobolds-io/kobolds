const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Parser = @import("../protocol/parser2.zig").Parser;
const Message = @import("../protocol/message2.zig").Message;

const constants = @import("../constants.zig");
const benchmark_constants = @import("./constants.zig");

const ParserParseBenchmark = struct {
    messages: []Message,
    parser: *Parser,
    bytes: []const u8,

    fn new(messages: []Message, parser: *Parser, bytes: []const u8) ParserParseBenchmark {
        return .{
            .messages = messages,
            .parser = parser,
            .bytes = bytes,
        };
    }

    pub fn run(self: ParserParseBenchmark, _: std.mem.Allocator) void {
        _ = self.parser.parse(self.messages, self.bytes) catch unreachable;
    }
};

fn afterEach() void {
    // reset the messages array list so we don't eat through all the memory on the machine
    // drop the len completely
    parser_messages.items.len = 0;
}

var parser_messages: std.ArrayList(Message) = undefined;

test "Parser benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = 100,
    });
    defer bench.deinit();

    var parser_messages_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = parser_messages_gpa.deinit();
    const parser_messages_allocator = parser_messages_gpa.allocator();

    parser_messages = std.ArrayList(Message).initCapacity(parser_messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer parser_messages.deinit(parser_messages_allocator);

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    var parser = try Parser.init(parser_allocator);
    defer parser.deinit(parser_allocator);

    const body = [_]u8{97} ** constants.message_max_body_size;
    var message_1 = Message.new(.undefined);
    message_1.setBody(&body);

    const message_1_buf = try parser_messages_allocator.alloc(u8, message_1.packedSize());
    defer parser_messages_allocator.free(message_1_buf);

    const serialized_bytes_1_count = message_1.serialize(message_1_buf);

    const bytes_1 = try parser_messages_allocator.alloc(u8, serialized_bytes_1_count * 1);
    defer parser_messages_allocator.free(bytes_1);
    const bytes_2 = try parser_messages_allocator.alloc(u8, serialized_bytes_1_count * 2);
    defer parser_messages_allocator.free(bytes_2);
    const bytes_3 = try parser_messages_allocator.alloc(u8, serialized_bytes_1_count * 3);
    defer parser_messages_allocator.free(bytes_3);

    const serialized_message = message_1_buf[0..serialized_bytes_1_count];
    @memcpy(bytes_1, serialized_message);

    @memcpy(bytes_2[0..serialized_bytes_1_count], serialized_message);
    @memcpy(bytes_2[serialized_bytes_1_count .. serialized_bytes_1_count * 2], serialized_message);

    @memcpy(bytes_3[0..serialized_bytes_1_count], serialized_message);
    @memcpy(bytes_3[serialized_bytes_1_count .. serialized_bytes_1_count * 2], serialized_message);
    @memcpy(bytes_3[serialized_bytes_1_count * 2 .. serialized_bytes_1_count * 3], serialized_message);

    const recv_buffer = try parser_messages_allocator.alloc(u8, constants.connection_recv_buffer_size);
    defer parser_messages_allocator.free(recv_buffer);

    // write as many messages as possible into the recv_buffer. This is the closest represenation of what the parser
    // would have to deal with for each connection
    var recv_buffer_index: usize = 0;
    while (recv_buffer[recv_buffer_index..].len >= bytes_1.len) {
        @memcpy(recv_buffer[recv_buffer_index .. recv_buffer_index + bytes_1.len], bytes_1);
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
    var message_2 = Message.new(.undefined);
    const message_2_buf = try parser_messages_allocator.alloc(u8, message_2.packedSize());
    defer parser_messages_allocator.free(message_2_buf);

    const serialized_bytes_2_count = message_2.serialize(message_2_buf);

    const bytes_4 = try parser_messages_allocator.alloc(u8, message_2.packedSize());
    defer parser_messages_allocator.free(bytes_4);
    @memcpy(bytes_4[0..serialized_bytes_2_count], message_2_buf[0..serialized_bytes_2_count]);

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

    var messages: [10]Message = undefined;

    try bench.addParam(
        parser_parse_4_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_4),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_1_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_1),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_2_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_2),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_3_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_3),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );
    try bench.addParam(
        parser_parse_5_title,
        &ParserParseBenchmark.new(&messages, &parser, recv_buffer[0..recv_buffer_index]),
        .{
            .hooks = .{
                .after_each = afterEach,
            },
        },
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|--------------------|\n");
    try writer.writeAll("| Parser2 Benchmarks |\n");
    try writer.writeAll("|--------------------|\n");
    try bench.run(writer);
}
