const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.Parser);

const constants = @import("../constants.zig");

const ProtocolError = @import("../errors.zig").ProtocolError;
const Message = @import("../protocol/message.zig").Message;
const Headers = @import("../protocol/message.zig").Headers;

pub const Parser = struct {
    const Self = @This();

    buffer: std.array_list.Managed(u8),

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .buffer = std.array_list.Managed(u8).initCapacity(allocator, constants.parser_max_buffer_size) catch unreachable,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    /// parse n messages from a data set. return the number of bytes successfully parsed from the data
    pub fn parse(self: *Self, messages: *std.array_list.Managed(Message), data: []const u8) !void {
        if (self.buffer.items.len + data.len > constants.parser_max_buffer_size) {
            return error.BufferOverflow;
        }

        // append the new data to the parser buffer
        try self.buffer.appendSlice(data);

        // if able to append new data to the buffer loop over the data in the buffer
        while (self.buffer.items.len >= @sizeOf(Headers)) {
            // we are going to try and parse another message out of the buffer
            var message = Message.new();
            message.decode(self.buffer.items) catch |err| switch (err) {
                ProtocolError.NotEnoughData => return,
                ProtocolError.InvalidHeadersChecksum, ProtocolError.InvalidBodyChecksum => {
                    // try to gracefully recover from this state
                    std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[1..]);
                    self.buffer.items.len -= 1;
                    continue;
                },
                else => return err,
            };

            try messages.append(message);

            // remove the parsed message from the buffer
            std.mem.copyForwards(u8, self.buffer.items, self.buffer.items[message.size()..]);
            self.buffer.items.len -= message.size();
        }
    }
};
