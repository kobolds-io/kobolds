const std = @import("std");
const zbench = @import("zbench");

const Parser = @import("../protocol/parser.zig").Parser;
const Message = @import("../protocol/message.zig").Message;
const constants = @import("../protocol/constants.zig");

fn benchmarkParser(allocator: std.mem.Allocator) void {
    const body = comptime "a" ** constants.max_message_body_size;

    var original_message = Message.new();
    original_message.setBody(body);

    const encoded_message = Message.encode(allocator, &original_message) catch @panic("could not encode message");

    defer allocator.free(encoded_message);

    var messages_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer messages_arena.deinit();
    const messages_allocator = messages_arena.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser_buf: [constants.max_parser_buffer_size]u8 = undefined;
    var parser_fba = std.heap.FixedBufferAllocator.init(&parser_buf);
    const parser_fba_allocator = parser_fba.allocator();

    var parser = Parser.init(parser_fba_allocator);
    defer parser.deinit();

    parser.parse(&messages, encoded_message) catch @panic("could not parse message");
}
