const std = @import("std");
const Message = @import("./message.zig").Message;

pub const Mailbox = struct {
    const Self = @This();

    messages: std.ArrayList(Message),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Mailbox{
            .messages = std.ArrayList(Message).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.messages.deinit();
    }
};
