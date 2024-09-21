const std = @import("std");
const Connection = @import("./connection.zig").Connection;

const Message = @import("./message.zig").Message;
const Mailbox = @import("./mailbox.zig").Mailbox;

// The message bus is the main sender/receiver of messages.
pub const MessageBus = struct {
    const Self = @This();

    /// allocator used to put messages on the heap
    allocator: std.mem.Allocator,

    // TODO: make this a circular buffer OR a linked list storing messages.
    //      the current ring buffer that is included in the std lib is only able to handle bytes
    //      I should make my own implementation that can accept messages
    mailbox: Mailbox(*Message),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .mailbox = Mailbox(*Message).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.mailbox.deinit();
    }

    /// get the next message from the bus
    pub fn next(self: *Self) ?Message {
        self.mailbox.mutex.lock();
        defer self.mailbox.mutex.unlock();

        return self.mailbox.popHead();
    }

    pub fn append(self: *Self, message: *Message) !void {
        self.mailbox.mutex.lock();
        defer self.mailbox.mutex.unlock();

        try self.mailbox.append(message);
    }
};
