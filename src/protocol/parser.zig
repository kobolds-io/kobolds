const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.Parser);
const testing = std.testing;

const constants = @import("../constants.zig");

const Message = @import("./message.zig").Message;
const FixedHeaders = @import("./message.zig").FixedHeaders;
const ExtensionHeaders = @import("./message.zig").ExtensionHeaders;

pub const ParseError = error{
    BufferOverflow,
    OutOfMemory,
};

pub const Parser = struct {
    const Self = @This();

    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) !Parser {
        return Self{
            .buffer = try std.ArrayList(u8).initCapacity(allocator, constants.parser_max_buffer_size),
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.buffer.deinit(allocator);
    }

    pub fn parse(self: *Self, messages: []Message, data: []const u8) ParseError!usize {
        if (self.buffer.items.len + data.len > constants.parser_max_buffer_size) return ParseError.BufferOverflow;

        self.buffer.appendSliceAssumeCapacity(data);

        var count: usize = 0;
        var read_offset: usize = 0;

        while (count < messages.len) {
            const buf = self.buffer.items;
            if (buf.len - read_offset < FixedHeaders.packedSize()) break;

            const parsed = Message.deserialize(buf[read_offset..]) catch |err| switch (err) {
                error.Truncated => break,
                error.InvalidMessageType, error.InvalidTopicName, error.InvalidMessage, error.InvalidChecksum => {
                    read_offset += 1;
                    continue;
                },
            };

            if (parsed.bytes_consumed == 0) {
                log.err("buggy deserialize", .{});
                read_offset += 1;
                continue;
            }

            if (read_offset + parsed.bytes_consumed > buf.len) {
                log.err("mismatched bytes", .{});
                read_offset += 1;
                continue;
            }

            messages[count] = parsed.message;
            count += 1;
            read_offset += parsed.bytes_consumed;
        }

        // while (count < messages.len) {
        //     if (buf.len - read_offset < FixedHeaders.packedSize()) break;

        //     const parsed = Message.deserialize(buf[read_offset..]) catch |err| switch (err) {
        //         error.Truncated => break,
        //         error.InvalidMessageType, error.InvalidTopicName, error.InvalidMessage, error.InvalidChecksum => {
        //             // log.err("parse err {any}", .{err});
        //             // log.err("parser.buffer.items.len: {}, items: {any}", .{ self.buffer.items[read_offset..].len, self.buffer.items[read_offset..] });
        //             read_offset += 1; // skip bad byte
        //             @panic("ahhh");

        //             // continue;
        //         },
        //     };

        //     // protect against buggy deserialize
        //     if (parsed.bytes_consumed == 0) {
        //         log.err("buggy deserialize", .{});
        //         read_offset += 1;
        //         continue;
        //     }

        //     // parsed says it consumed more than available — treat as corrupted, drop a byte and continue to resync.
        //     if (read_offset + parsed.bytes_consumed > buf.len) {
        //         log.err("mismatched bytes", .{});
        //         read_offset += 1;
        //         continue;
        //     }

        //     messages[count] = parsed.message;
        //     count += 1;

        //     // advance the read offset
        //     read_offset += parsed.bytes_consumed;
        // }

        // Remove consumed bytes from buffer
        if (read_offset > 0) {
            std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[read_offset..]);
            self.buffer.items.len -= read_offset;
        }

        return count;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var parser = try Parser.init(allocator);
    defer parser.deinit(allocator);
}

test "parser.parse" {
    const allocator = testing.allocator;

    // Create a message
    const body = [_]u8{97} ** constants.message_max_body_size;
    var message = Message.new(.unsupported);
    message.setBody(&body);

    const buf = try allocator.alloc(u8, message.packedSize());
    defer allocator.free(buf);

    // serialize the test message to be parsed to simulate a message coming in off the network
    const n = message.serialize(buf);

    var parser = try Parser.init(allocator);
    defer parser.deinit(allocator);

    // Stack allocated reusable list of messages
    var messages: [10]Message = undefined;

    _ = try parser.parse(&messages, buf[0..n]);
}

// test "parse parses multiple messages" {
//     const want_body = "a" ** constants.message_max_body_size;

//     const allocator = testing.allocator;

//     var message = Message.new(.publish);
//     message.setTopicName("/test");
//     message.setBody(want_body);

//     const buf = try allocator.alloc(u8, message.packedSize());
//     defer allocator.free(buf);

//     _ = message.serialize(buf);

//     // make a buffer that could fit 10 messages
//     var data_buf: [@sizeOf(Message) * 10]u8 = undefined;
//     var fba = std.heap.FixedBufferAllocator.init(&data_buf);
//     const fba_allocator = fba.allocator();

//     var data = try std.ArrayList(u8).initCapacity(fba_allocator, data_buf.len);
//     defer data.deinit(fba_allocator);

//     for (0..5) |_| {
//         // we append the same encoded message 5 times
//         try data.appendSlice(allocator, buf);
//     }

//     var parser = try Parser.init(allocator);
//     defer parser.deinit(allocator);

//     // Stack allocated reusable list of messages
//     var messages: [10]Message = undefined;

//     const consumed = try parser.parse(&messages, data.items);

//     try std.testing.expectEqual(5, messages[0..consumed].len);
//     try testing.expect(std.mem.eql(u8, want_body, messages[0].body()));
//     try testing.expect(std.mem.eql(u8, want_body, messages[1].body()));
//     try testing.expect(std.mem.eql(u8, want_body, messages[2].body()));
//     try testing.expect(std.mem.eql(u8, want_body, messages[3].body()));
//     try testing.expect(std.mem.eql(u8, want_body, messages[4].body()));

//     // we know that we parsed EVERYTHING so there should be no more bytes in the buffer
//     try testing.expectEqual(0, parser.buffer.items.len);
// }
