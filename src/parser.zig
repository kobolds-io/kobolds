const std = @import("std");
const assert = std.debug.assert;
const constants = @import("./constants.zig");
const log = std.log.scoped(.Parser);

const ProtocolError = @import("./errors.zig").ProtocolError;
const Message = @import("./message.zig").Message;
const Headers = @import("./message.zig").Headers;

pub const Parser = struct {
    const Self = @This();

    buffer: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .buffer = std.ArrayList(u8).initCapacity(allocator, constants.connection_recv_buffer_size) catch unreachable,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    /// parse n messages from a data set. return the number of bytes successfully parsed from the data
    pub fn parse(self: *Self, messages: *std.ArrayList(Message), data: []const u8) !void {

        // append the new data to the parser buffer
        try self.buffer.appendSlice(data);

        // if able to append new data to the buffer
        // loop over the data in the buffer
        while (self.buffer.items.len >= @sizeOf(Headers)) {
            // we are going to try and parse another message out of the buffer
            var message = Message.new();
            message.decode(self.buffer.items) catch |err| {
                // find the next non-zero character
                log.debug("could not decode message {any}", .{err});
                return err;
            };

            // append the message to the messages
            try messages.append(message);

            // remove the parsed message from the buffer
            std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[message.size()..]);
            self.buffer.items.len -= message.size();
        }
    }
};

pub const Parser2 = struct {
    const Self = @This();

    buffer: [constants.message_max_size * 5]u8,
    index: usize,

    pub fn new() Parser2 {
        return Parser2{
            .buffer = undefined,
            .index = 0,
        };
    }

    // parse n message from a data set
    pub fn parse(self: *Self, messages: *std.ArrayList(Message), data: []const u8) !void {
        // append the new data to the parser buffer
        // assert(self.index >= 0 and self.index <= self.buffer.len);
        // assert(data.len <= self.buffer[self.index..].len);

        // copy the data to the internal buffer
        @memcpy(self.buffer[self.index .. self.index + data.len], data);
        // std.mem.copyForwards(u8, self.buffer[self.index .. self.index + data.len], data);
        self.index += data.len;

        // if able to append new data to the buffer
        // loop over the data in the buffer
        while (self.buffer[0..self.index].len >= @sizeOf(Headers)) {
            // we are going to try and parse another message out of the buffer
            var message = Message.new();

            message.decode(self.buffer[0..self.index]) catch |err| {
                std.log.debug("could not decode message {any}", .{err});
                return;
            };

            // decrement the index for the size of the message since it was already processed
            // this should be done before appending the message because appending the message
            // can fail whereas decrementing the index should never fail
            self.index -= message.size();

            // append the message to the messages
            try messages.append(message);

            // remove the parsed message from the buffer
            std.mem.copyForwards(u8, &self.buffer, self.buffer[message.size()..]);
        }
    }
};
