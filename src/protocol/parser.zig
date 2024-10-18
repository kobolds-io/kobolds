const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ProtocolError = @import("./errors.zig").ProtocolError;
const Message = @import("./message.zig").Message;
const Header = @import("./message.zig").Header;

pub const Parser = struct {
    const Self = @This();

    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .buffer = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    // Deprecated and shouldn't be used
    pub fn parseMessage(self: *Self, allocator: std.mem.Allocator, data: []const u8) ?*Message {
        assert(data.len <= constants.max_parser_buffer_size);

        // allow if:
        //  the incoming data is large enough to hold the header
        //  OR there are already bytes in the buffer
        assert(data.len >= @sizeOf(Header) or self.buffer.items.len > 0);

        // read the bytes for the header out of the buffer
        // copy the data to the buffer
        self.buffer.appendSlice(data) catch |err| {
            std.log.debug("could not append data to the parser buffer {any}", .{err});
            std.log.debug("clearing the buffer {any}", .{err});
            self.buffer.clearAndFree();

            return null;
        };

        const message = Message.decode(allocator, self.buffer.items) catch |err| {
            if (err == ProtocolError.NotEnoughData) {
                return null;
            } else {
                // something terrible has happened and we are unable to parse the message
                // reset the world!
                std.log.debug("unable to parse message {any}", .{err});
                self.buffer.clearAndFree();
                return null;
            }
        };

        // remove the parsed message from the buffer
        std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[message.size()..]);
        self.buffer.items.len -= message.size();

        return message;
    }

    // parse n message from a data set
    pub fn parse(self: *Self, messages: *std.ArrayList(Message), data: []const u8) !void {
        // append the new data to the parser buffer
        try self.buffer.appendSlice(data);

        // if able to append new data to the buffer
        // loop over the data in the buffer
        while (self.buffer.items.len >= @sizeOf(Header)) {
            // we are going to try and parse another message out of the buffer
            var message: Message = undefined;
            Message.decode_(&message, self.buffer.items) catch |err| {
                std.log.debug("could not decode_ message {any}", .{err});
                return;
            };

            // append the message to the messages
            try messages.append(message);

            // remove the parsed message from the buffer
            std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[message.size()..]);
            self.buffer.items.len -= message.size();
        }
    }
};

test "parse parses a message" {
    const want_body = "a" ** constants.max_message_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try Message.encode(allocator, &message);
    defer allocator.free(encoded_message);

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).init(messages_allocator);
    defer messages.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    try std.testing.expectEqual(0, messages.items.len);

    // ACT
    try parser.parse(&messages, encoded_message);

    try std.testing.expectEqual(1, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
}

test "parse parses multiple messages" {
    const want_body = "a" ** constants.max_message_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    var data_buf: [1024 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&data_buf);
    const fba_allocator = fba.allocator();

    var data = std.ArrayList(u8).init(fba_allocator);

    const encoded_message = try Message.encode(allocator, &message);
    defer allocator.free(encoded_message);

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

    // ACT
    try parser.parse(&messages, data.items);

    try std.testing.expectEqual(5, messages.items.len);
    try testing.expect(std.mem.eql(u8, want_body, messages.items[0].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[1].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[2].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[3].body()));
    try testing.expect(std.mem.eql(u8, want_body, messages.items[4].body()));
}

test "parse parses messages fast" {
    const allocator = testing.allocator;

    const ITERATIONS = 3_500_000;

    const body = comptime "a" ** constants.max_message_body_size;

    var original_message = Message.new();
    original_message.setBody(body);

    const encoded_message = try Message.encode(allocator, &original_message);
    defer allocator.free(encoded_message);

    var messages_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer messages_arena.deinit();
    const messages_allocator = messages_arena.allocator();

    var messages = try std.ArrayList(Message).initCapacity(messages_allocator, ITERATIONS);
    defer messages.deinit();

    var parser_buf: [constants.max_parser_buffer_size]u8 = undefined;
    var parser_fba = std.heap.FixedBufferAllocator.init(&parser_buf);
    const parser_fba_allocator = parser_fba.allocator();

    var parser = Parser.init(parser_fba_allocator);
    defer parser.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    var durations_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const durations_allocator = durations_arena.allocator();
    defer durations_arena.deinit();

    var durations = try std.ArrayList(u64).initCapacity(durations_allocator, ITERATIONS);
    defer durations.deinit();

    for (0..ITERATIONS) |_| {
        const parse_start = timer.read();

        try parser.parse(&messages, encoded_message);

        const parse_end = timer.read();
        durations.appendAssumeCapacity(parse_end - parse_start);
    }

    const end = timer.read();

    var duration_sum: u128 = 0;
    for (durations.items) |dur| {
        duration_sum += @intCast(dur);
    }

    const average_duration = duration_sum / ITERATIONS;

    // std.log.debug("average parser.parse duration: {any}ns", .{average_duration});
    // std.log.debug("average parser.parse duration: {any}us", .{average_duration / std.time.ns_per_us});
    // std.log.debug("average parser.parse duration: {any}ms", .{average_duration / std.time.ns_per_ms});
    // std.log.debug("parser.parse took timer.read: {any}ns", .{end});
    // std.log.debug("parser.parse took timer.read: {any}us", .{end / std.time.ns_per_us});
    // std.log.debug("parser.parse took timer.read: {any}ms", .{end / std.time.ns_per_ms});

    try testing.expect(average_duration / std.time.ns_per_ms < 1);
    try testing.expect(end / std.time.ns_per_ms < 2500);
    try testing.expectEqual(ITERATIONS, messages.items.len);
}

test "parseMessage parses a message" {
    const want_body = "a" ** constants.max_message_body_size;

    const allocator = testing.allocator;

    var message = Message.new();
    message.setBody(want_body);

    const encoded_message = try Message.encode(allocator, &message);
    defer allocator.free(encoded_message);

    var parser = Parser.init(allocator);
    defer parser.deinit();

    var parsed_message = parser.parseMessage(allocator, encoded_message).?;
    defer allocator.destroy(parsed_message);

    try testing.expect(std.mem.eql(u8, want_body, parsed_message.body()));
}

test "parseMessage it returns an error if the data only includes partial data" {
    const want_body = "a" ** constants.max_message_body_size;

    const allocator = testing.allocator;

    var original_message = Message.new();

    // set the body to be tested
    original_message.setBody(want_body);

    const encoded_message = try Message.encode(allocator, &original_message);
    defer allocator.free(encoded_message);

    // grab just the header portion of the message
    const partial_1: []const u8 = encoded_message[0..@sizeOf(Header)];
    // grab the remainder of the message
    const partial_2: []const u8 = encoded_message[@sizeOf(Header)..];

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // create an array of buffers
    var buffers = std.ArrayList([]const u8).init(allocator);
    defer buffers.deinit();

    try buffers.append(partial_1);
    try buffers.append(partial_2);

    try testing.expectEqual(2, buffers.items.len);

    var parsed_message: ?*Message = null;

    // this is a poor man's simulation of a stream
    for (buffers.items, 0..) |buf, i| {
        parsed_message = parser.parseMessage(allocator, buf) orelse continue;
        defer allocator.destroy(parsed_message.?);

        // we should be seeing the parsed message only on the second iteration
        try testing.expectEqual(1, i);
        try testing.expect(std.mem.eql(u8, want_body, parsed_message.?.body()));
        try testing.expectEqual(0, parser.buffer.items.len);
        return;
    }

    // we should not reach this if the parser was successful
    unreachable;
}

test "parseMessage parses messages fast" {
    const allocator = testing.allocator;

    const ITERATIONS = 3_500_000;

    const body = comptime "a" ** constants.max_message_body_size;

    var original_message = Message.new();
    original_message.setBody(body);

    const encoded_message = try Message.encode(allocator, &original_message);
    defer allocator.free(encoded_message);

    var decoded_messages_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer decoded_messages_arena.deinit();
    const decoded_messages_allocator = decoded_messages_arena.allocator();

    var decoded_messages = std.ArrayList(*Message).init(decoded_messages_allocator);
    defer decoded_messages.deinit();

    var parser_buf: [constants.max_parser_buffer_size]u8 = undefined;
    var parser_fba = std.heap.FixedBufferAllocator.init(&parser_buf);
    const parser_fba_allocator = parser_fba.allocator();

    var parser = Parser.init(parser_fba_allocator);
    defer parser.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    var durations_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const durations_allocator = durations_arena.allocator();
    defer durations_arena.deinit();

    var durations = try std.ArrayList(u64).initCapacity(durations_allocator, ITERATIONS);
    defer durations.deinit();

    for (0..ITERATIONS) |_| {
        var buf: [constants.max_message_size + 100]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const fba_allocator = fba.allocator();

        const parse_start = timer.read();

        const parsed_message_opt = parser.parseMessage(fba_allocator, encoded_message);
        if (parsed_message_opt != null) {
            const parse_end = timer.read();
            durations.appendAssumeCapacity(parse_end - parse_start);
            try decoded_messages.append(parsed_message_opt.?);
        } else unreachable;
    }

    const end = timer.read();

    var duration_sum: u128 = 0;
    for (durations.items) |dur| {
        duration_sum += @intCast(dur);
    }

    const average_duration = duration_sum / ITERATIONS;

    // std.log.debug("average parser.parseMessage duration: {any}n", .{average_duration});
    // std.log.debug("average parser.parseMessage duration: {any}u", .{average_duration / std.time.ns_per_us});
    // std.log.debug("average parser.parseMessage duration: {any}m", .{average_duration / std.time.ns_per_ms});
    // std.log.debug("parser.parseMessage took timer.read: {any}ns, .{end});
    // std.log.debug("parser.parseMessage took timer.read: {any}us, .{end / std.time.ns_per_us});
    // std.log.debug("parser.parseMessage took timer.read: {any}ms, .{end / std.time.ns_per_ms});

    try testing.expect(average_duration / std.time.ns_per_ms < 1);
    try testing.expect(end / std.time.ns_per_ms < 2500);
    try testing.expectEqual(ITERATIONS, decoded_messages.items.len);
}
