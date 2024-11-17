const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ProtocolError = @import("./errors.zig").ProtocolError;
const Message = @import("./message.zig").Message;
const Headers = @import("./message.zig").Headers;

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

    // parse n message from a data set
    pub fn parse(self: *Self, messages: *std.ArrayList(Message), data: []const u8) !void {
        // append the new data to the parser buffer
        try self.buffer.appendSlice(data);

        // if able to append new data to the buffer
        // loop over the data in the buffer
        while (self.buffer.items.len >= @sizeOf(Headers)) {
            // we are going to try and parse another message out of the buffer
            var message = Message.new();
            message.decode(self.buffer.items) catch |err| {
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
