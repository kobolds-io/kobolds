const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ProtocolError = @import("./errors.zig").ProtocolError;
const Message = @import("./message.zig").Message;
const Headers = @import("./message.zig").Headers;
const Parser = @import("./parser.zig").Parser;
const Parser2 = @import("./parser.zig").Parser2;

test "parse parses a message" {
    const want_body = "a" ** constants.message_max_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    try std.testing.expectEqual(0, messages.items.len);

    try parser.parse(&messages, encoded_message);

    try std.testing.expectEqual(1, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
}

test "parse parses multiple messages" {
    const want_body = "a" ** constants.message_max_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    // make a buffer that could fit 10 messages
    var data_buf: [@sizeOf(Message) * 10]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&data_buf);
    const fba_allocator = fba.allocator();

    var data = std.ArrayList(u8).init(fba_allocator);

    for (0..5) |_| {
        // we append the same encoded message 5 times
        try data.appendSlice(encoded_message);
    }

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    try std.testing.expectEqual(0, messages.items.len);

    try parser.parse(&messages, data.items);

    try std.testing.expectEqual(5, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[1].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[2].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[3].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[4].body()));
}

test "parser2 parses a message" {
    const want_body = "a" ** constants.message_max_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser = Parser2.new();

    try std.testing.expectEqual(0, messages.items.len);

    try parser.parse(&messages, encoded_message);

    try std.testing.expectEqual(1, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
}

test "parser2 parses multiple messages" {
    const want_body = "a" ** constants.message_max_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    // make a buffer that could fit 10 messages
    var data_buf: [@sizeOf(Message) * 10]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&data_buf);
    const fba_allocator = fba.allocator();

    var data = std.ArrayList(u8).init(fba_allocator);

    for (0..5) |_| {
        // we append the same encoded message 5 times
        try data.appendSlice(encoded_message);
    }

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser = Parser2.new();

    try std.testing.expectEqual(0, messages.items.len);

    // ACT
    try parser.parse(&messages, data.items);

    try std.testing.expectEqual(5, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[1].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[2].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[3].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[4].body()));
}
