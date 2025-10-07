const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Parser = @import("../protocol/parser.zig").Parser;
const Message = @import("../protocol/message.zig").Message;

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
        assert(self.parser.buffer.items.len == 0);

        // Add the
        self.parser.buffer.appendSliceAssumeCapacity(self.bytes);

        var i: usize = 0;
        while (self.parser.buffer.items.len > 0 or i > 10) : (i += 1) {
            _ = self.parser.parse(self.messages, &.{}) catch unreachable;
        }

        if (self.parser.buffer.items.len > 0) {
            std.debug.print("parser didn't empty! {}\n", .{self.parser.buffer.items.len});
            unreachable;
        }
    }
};

var parser_messages: std.ArrayList(Message) = undefined;

test "Parser benchmarks" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{
        .iterations = benchmark_constants.benchmark_max_iterations,
    });
    defer bench.deinit();

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    var parser = try Parser.init(parser_allocator);
    defer parser.deinit(parser_allocator);

    const topic_name = [_]u8{98} ** constants.message_max_topic_name_size;
    const body = [_]u8{97} ** constants.message_max_body_size;
    var message_1 = Message.new(.publish);
    message_1.setTopicName(&topic_name);
    message_1.setBody(&body);

    const message_1_buf = try messages_allocator.alloc(u8, message_1.packedSize());
    defer messages_allocator.free(message_1_buf);

    const serialized_bytes_1_count = message_1.serialize(message_1_buf);

    const bytes_1 = try messages_allocator.alloc(u8, serialized_bytes_1_count * 1);
    defer messages_allocator.free(bytes_1);
    const bytes_2 = try messages_allocator.alloc(u8, serialized_bytes_1_count * 2);
    defer messages_allocator.free(bytes_2);
    const bytes_3 = try messages_allocator.alloc(u8, serialized_bytes_1_count * 3);
    defer messages_allocator.free(bytes_3);

    const serialized_message = message_1_buf[0..serialized_bytes_1_count];
    @memcpy(bytes_1, serialized_message);

    @memcpy(bytes_2[0..serialized_bytes_1_count], serialized_message);
    @memcpy(bytes_2[serialized_bytes_1_count .. serialized_bytes_1_count * 2], serialized_message);

    @memcpy(bytes_3[0..serialized_bytes_1_count], serialized_message);
    @memcpy(bytes_3[serialized_bytes_1_count .. serialized_bytes_1_count * 2], serialized_message);
    @memcpy(bytes_3[serialized_bytes_1_count * 2 .. serialized_bytes_1_count * 3], serialized_message);

    const recv_buffer = try messages_allocator.alloc(u8, constants.connection_recv_buffer_size);
    defer messages_allocator.free(recv_buffer);

    // write as many messages as possible into the recv_buffer. This is the closest represenation of what the parser
    // would have to deal with for each connection
    var recv_buffer_index: usize = 0;
    while (recv_buffer[recv_buffer_index..].len >= bytes_1.len) {
        @memcpy(recv_buffer[recv_buffer_index .. recv_buffer_index + bytes_1.len], bytes_1);
        recv_buffer_index += bytes_1.len;
    }

    const parser_parse_1_title = try std.fmt.allocPrint(
        messages_allocator,
        "parse {} bytes",
        .{bytes_1.len},
    );
    defer messages_allocator.free(parser_parse_1_title);

    const parser_parse_2_title = try std.fmt.allocPrint(
        messages_allocator,
        "parse {} bytes",
        .{bytes_2.len},
    );
    defer messages_allocator.free(parser_parse_2_title);

    const parser_parse_3_title = try std.fmt.allocPrint(
        messages_allocator,
        "parse {} bytes",
        .{bytes_3.len},
    );
    defer messages_allocator.free(parser_parse_3_title);

    // create a single encoded message with no body
    var message_2 = Message.new(.publish);
    message_2.setTopicName(&topic_name);
    const message_2_buf = try messages_allocator.alloc(u8, message_2.packedSize());
    defer messages_allocator.free(message_2_buf);

    const serialized_bytes_2_count = message_2.serialize(message_2_buf);

    const bytes_4 = try messages_allocator.alloc(u8, message_2.packedSize());
    defer messages_allocator.free(bytes_4);
    @memcpy(bytes_4[0..serialized_bytes_2_count], message_2_buf[0..serialized_bytes_2_count]);

    const parser_parse_4_title = try std.fmt.allocPrint(
        messages_allocator,
        "parse {} bytes",
        .{bytes_4.len},
    );
    defer messages_allocator.free(parser_parse_4_title);

    const parser_parse_5_title = try std.fmt.allocPrint(
        messages_allocator,
        "parse {} bytes",
        .{recv_buffer[0..recv_buffer_index].len},
    );
    defer messages_allocator.free(parser_parse_5_title);

    var messages: [10]Message = undefined;

    try bench.addParam(
        parser_parse_4_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_4),
        .{},
    );
    try bench.addParam(
        parser_parse_1_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_1),
        .{},
    );
    try bench.addParam(
        parser_parse_2_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_2),
        .{},
    );
    try bench.addParam(
        parser_parse_3_title,
        &ParserParseBenchmark.new(&messages, &parser, bytes_3),
        .{},
    );
    try bench.addParam(
        parser_parse_5_title,
        &ParserParseBenchmark.new(&messages, &parser, recv_buffer[0..recv_buffer_index]),
        .{},
    );

    var stderr = std.fs.File.stderr().writerStreaming(&.{});
    const writer = &stderr.interface;

    try writer.writeAll("\n");
    try writer.writeAll("|-------------------|\n");
    try writer.writeAll("| Parser Benchmarks |\n");
    try writer.writeAll("|-------------------|\n");
    try bench.run(writer);
}
