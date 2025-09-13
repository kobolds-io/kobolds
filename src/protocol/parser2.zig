const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.Parser);
const testing = std.testing;

const constants = @import("../constants.zig");

const Message = @import("./message2.zig").Message;
const FixedHeaders = @import("./message2.zig").FixedHeaders;
const ExtensionHeaders = @import("./message2.zig").ExtensionHeaders;

pub const ParseError = error{
    BufferOverflow,
    OutOfMemory,
};

pub const Parser = struct {
    const Self = @This();

    const ParseResult = struct {
        count: usize = 0,
        truncated: bool = false,
    };

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

        // Add the data to the internal buffer
        self.buffer.appendSliceAssumeCapacity(data);

        var i: usize = 0;
        // var read_offset: usize = 0;

        // check if there is enough data to at least fit fixed headers
        while (self.buffer.items.len >= FixedHeaders.packedSize() and i < messages.len) : (i += 1) {
            // pull out the data for the fixed headers
            const fixed_headers = FixedHeaders.fromBytes(self.buffer.items[0..FixedHeaders.packedSize()]) catch unreachable;

            // FIX: this actually is incorrect since topics are variable length.
            const extension_size = switch (fixed_headers.message_type) {
                .undefined => 0,
                .publish => (ExtensionHeaders{ .publish = .{} }).packedSize(),
                .subscribe => (ExtensionHeaders{ .subscribe = .{} }).packedSize(),
            };
            const packed_message_size = FixedHeaders.packedSize() + extension_size + fixed_headers.body_length + 8;

            if (self.buffer.items.len >= packed_message_size) {
                const message = Message.deserialize(self.buffer.items[0..packed_message_size]) catch |err| switch (err) {
                    error.Truncated => {
                        // there were not enough bytes in the buffer to parse the full message. There
                        // is nothing left for us to do here except move on
                        return i;
                    },
                    error.InvalidMessage, error.InvalidChecksum => {
                        // The message could not be parse so we are going to just drop the first byte
                        // and walk the buffer forward in order to gracefully recover. This is not the most efficient
                        // mechanism to to keep the parser moving but.
                        std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[1..]);
                        self.buffer.items.len -= 1;
                        continue;
                    },
                    else => {
                        log.err("unhandled error: {any}", .{err});
                        unreachable;
                    },
                };

                messages[i] = message;
                std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[packed_message_size..]);
                self.buffer.items.len -= packed_message_size;
                continue;
            }

            // There are not enough bytes in the buffer to be able to parse a full message. At this
            // point all messages that are parseable have been parsed
            break;
        }

        return i;
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
    var message = Message.new(.undefined);
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

// pub const Parser = struct {
//     const Self = @This();

//     buffer: std.array_list.Managed(u8),

//     pub fn init(allocator: std.mem.Allocator) Parser {
//         return Parser{
//             .buffer = std.array_list.Managed(u8).initCapacity(allocator, constants.parser_max_buffer_size) catch unreachable,
//         };
//     }

//     pub fn deinit(self: *Self) void {
//         self.buffer.deinit();
//     }

//     /// parse n messages from a data set. return the number of bytes successfully parsed from the data
//     pub fn parse(self: *Self, messages: *std.array_list.Managed(Message), data: []const u8) !void {
//         if (self.buffer.items.len + data.len > constants.parser_max_buffer_size) {
//             return error.BufferOverflow;
//         }

//         // append the new data to the parser buffer
//         try self.buffer.appendSlice(data);

//         // if able to append new data to the buffer loop over the data in the buffer
//         while (self.buffer.items.len >= @sizeOf(Headers)) {
//             // we are going to try and parse another message out of the buffer
//             var message = Message.new();
//             message.decode(self.buffer.items) catch |err| switch (err) {
//                 ProtocolError.NotEnoughData => return,
//                 ProtocolError.InvalidHeadersChecksum, ProtocolError.InvalidBodyChecksum => {
//                     // try to gracefully recover from this state
//                     std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[1..]);
//                     self.buffer.items.len -= 1;
//                     continue;
//                 },
//                 else => return err,
//             };

//             try messages.append(message);

//             // remove the parsed message from the buffer
//             std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[message.size()..]);
//             self.buffer.items.len -= message.size();
//         }
//     }
// };
