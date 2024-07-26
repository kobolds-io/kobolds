const std = @import("std");
const Message = @import("./message.zig").Message;

// TODO: low priority - This is currently hardcoded to only use messages
//      i want to make this generic to use any kind of type.
pub const Mailbox = struct {
    const Self = @This();

    // TODO: convert this to a linked list instead so prepending / appending is faster
    // should have a reference to the head and tail so adding and removing messages is fast.
    // It also needs to be on the heap because messages must be passed around.
    messages: std.ArrayList(Message),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .messages = std.ArrayList(Message).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.messages.deinit();
    }

    pub fn append(self: *Self, message: Message) !void {
        try self.messages.append(message);
    }

    // This is a slow an inefficient function due to the data structure used. A linked list would be pretty good but
    pub fn popHead(self: *Self) !?Message {
        // remove the first item from the list
        if (self.messages.items.len == 0) return null;
        if (self.messages.items.len == 1) return self.messages.pop();

        // remove the first item out of the array
        const message = self.messages.items[0];
        const n = 1;

        std.mem.copyForwards(Message, self.messages.items, self.messages.items[n..]);
        self.messages.items.len -= n;

        return message;
    }
};
