const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const Message = @import("../protocol/message.zig").Message;
const constants = @import("../constants.zig");

pub const QueueError = error{
    QueueFull,
    QueueEmpty,
    NotFound,
};

/// A simple linked list queue implementation that uses the Message.next field to traverse and track usage
pub const MessageQueue = struct {
    const Self = @This();

    head: ?*Message,
    tail: ?*Message,

    /// this is used internally to track the size of the queue, don't modify outside of queue
    count: u32,
    max_size: u32,

    pub fn new(max_size: ?u32) Self {
        var max_size_ = constants.queue_size_default;
        if (max_size) |n| {
            assert(n <= constants.queue_size_max);
            max_size_ = n;
        }

        // ensure that the
        return Self{
            .head = null,
            .tail = null,
            .count = 0,
            .max_size = max_size_,
        };
    }

    pub fn isEmpty(self: *Self) bool {
        if (self.count == 0) {
            assert(self.head == null);
            assert(self.tail == null);
            return true;
        }

        // if one of these fail, that means that the count is borked and we have a
        // logic error somewhere in one of the operations so someone has modified
        // the count outside of the queue
        assert(self.head != null);
        assert(self.tail != null);

        return false;
    }

    /// drops all references and safely resets the queue.
    pub fn reset(self: *Self) void {
        if (self.isEmpty()) return;

        var current = self.head;
        while (current) |message| {
            const tmp = message.next;
            self.head = tmp;
            message.next = null;
            self.count -= 1;
            current = tmp;
        }

        std.debug.print("reset count {}\n", .{self.count});

        assert(self.head == null);
        assert(self.count == 0);
        self.tail = null;
    }

    pub fn prepend(self: *Self, message: *Message) QueueError!void {
        assert(message.next == null);

        if (self.count == self.max_size) {
            return QueueError.QueueFull;
        }

        defer self.count += 1;

        if (self.isEmpty()) {
            self.head = message;
            self.tail = message;
            return;
        }

        assert(self.head != null);

        const tmp = self.head.?;
        message.next = tmp;
        self.head = message;
        return;
    }

    pub fn enqueue(self: *Self, message: *Message) QueueError!void {
        // ensure that the new item is fresh and not referencing anything
        assert(message.next == null);

        if (self.count == self.max_size) {
            return QueueError.QueueFull;
        }

        // always increment the count
        defer self.count += 1;

        // handle the case the list is empty;
        if (self.isEmpty()) {
            self.head = message;
            self.tail = message;
            return;
        }

        // we know that the tail is not null here;
        const temp_tail = self.tail.?;
        temp_tail.next = message;
        self.tail = message;
    }

    /// Enqueue many messages at a time. This is faster than calling enqueue one at a time
    pub fn enqueueMany(self: *Self, messages: []const *Message) QueueError!void {
        // do nothing if the incoming messages is actually empty
        if (messages.len == 0) return;

        // check if adding these messages would overflow the count
        if (self.count + messages.len > self.max_size) return QueueError.QueueFull;

        // we are just going to loop over each message and assign next to the next message
        var current: *Message = undefined;
        for (0..messages.len) |i| {
            current = messages[i];
            if (i + 1 < messages.len) {
                current.next = messages[i + 1];
            }
        }

        // handle the case where the head is empty
        if (self.isEmpty()) {
            self.head = messages[0];
        }

        // increase the count by the enqueue messages count
        self.count += @intCast(messages.len);

        // set the tail to the last message
        self.tail = messages[messages.len - 1];
    }

    /// Remove the first item from the queue
    pub fn dequeue(self: *Self) ?*Message {
        if (self.isEmpty()) return null;

        // it is validated above that head is not null
        var result = self.head.?;
        self.head = result.next;

        self.count -= 1;

        // FIX: this shouldn't need to happen.
        if (self.head == null) {
            self.count = 0;
        }

        // zero out the queue completely
        if (self.count == 0) {
            self.head = null;
            self.tail = null;
        }

        result.next = null;
        return result;
    }

    /// This function completely clears the queue. This does not perform any safety checks
    pub fn clear(self: *Self) void {
        // there is nothing to do
        if (self.isEmpty()) return;

        self.head = null;
        self.tail = null;
        self.count = 0;
    }

    /// This is an unsafe function that appends ALL messages in the other queue
    /// It does NOT clear the `other` queue. Do that with `MessageQueue.clear`
    pub fn concatenate(self: *Self, other: *MessageQueue) void {
        // there is nothing to do
        if (other.isEmpty()) return;

        // ensure that the new messages will fit into this queue
        assert(self.count + other.count <= self.max_size);

        if (self.isEmpty()) {
            self.head = other.head;
            self.tail = other.tail;
            self.count = other.count;
            return;
        }

        if (self.head == null) unreachable;
        if (self.tail == null) unreachable;

        self.tail.?.next = other.head;
        self.tail = other.tail;
        self.count += other.count;

        assert(self.head != null);
        assert(self.tail != null);
        assert(self.count <= self.max_size);
    }

    /// Dequeue many items at a time, this is just as fast as calling dequeue individually
    pub fn dequeueMany(self: *Self, buf: []*Message) u32 {
        if (self.isEmpty()) return 0;

        var count: u32 = 0;
        for (0..buf.len) |i| {
            if (self.dequeue()) |m| {
                buf[i] = m;
                count += 1;
            } else {
                return count;
            }
        }

        return count;
    }

    // To all the CS people out there, i am sorry for this monstrosity
    pub fn remove(self: *Self, ptr: *Message) !void {
        if (self.isEmpty()) return QueueError.QueueEmpty;

        // this is safe because we check for empty right above
        if (self.head.? == ptr) {
            self.count -= 1;
            self.head = ptr.next;
            ptr.next = null; // remove the ref to the next message

            if (self.count == 1) {
                self.tail = self.head;
            } else if (self.count == 0) {
                self.tail = null;
                self.head = null;
            }

            return;
        }

        // we have already checked the head and can now check the list
        var prev: *Message = self.head.?;
        var current: ?*Message = self.head.?.next;

        var i: u32 = 0;
        while (current) |message| : (i += 1) {
            if (message == ptr) {
                prev.next = ptr.next;
                ptr.next = null; // remove the ref to the next message

                self.count -= 1;

                // handle the special case that this is the tail
                if (i + 1 == self.count) {
                    self.tail = prev;
                }

                break;
            }

            // advance the search
            prev = message;
            current = message.next;
        } else return QueueError.NotFound;

        if (self.count == 0) {
            self.tail = null;
            self.head = null;
        }
    }
};

test "enqueue/dequeue" {
    var m1 = Message.new();
    var m2 = Message.new();
    var m3 = Message.new();
    var m4 = Message.new();

    var queue = MessageQueue.new(3);
    try queue.enqueue(&m1);

    try std.testing.expectEqual(1, queue.count);
    try std.testing.expectEqual(queue.head, &m1);

    try queue.enqueue(&m2);

    try std.testing.expectEqual(2, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(queue.head.?.next.?, &m2);
    try std.testing.expectEqual(queue.tail, &m2);

    try queue.enqueue(&m3);

    try std.testing.expectEqual(3, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(queue.tail, &m3);

    try std.testing.expectError(QueueError.QueueFull, queue.enqueue(&m4));

    const qm1 = queue.dequeue() orelse unreachable;

    try std.testing.expectEqual(2, queue.count);
    try std.testing.expectEqual(&m1, qm1);
    try std.testing.expectEqual(queue.head, &m2);

    const qm2 = queue.dequeue() orelse unreachable;

    try std.testing.expectEqual(1, queue.count);
    try std.testing.expectEqual(&m2, qm2);
    try std.testing.expectEqual(queue.head, &m3);

    const qm3 = queue.dequeue() orelse unreachable;

    try std.testing.expectEqual(0, queue.count);
    try std.testing.expectEqual(&m3, qm3);
    try std.testing.expectEqual(queue.head, null);
    try std.testing.expectEqual(queue.tail, null);

    try std.testing.expectEqual(null, queue.dequeue());
}

test "prepend" {
    var m1 = Message.new();
    var m2 = Message.new();
    var m3 = Message.new();
    var m4 = Message.new();

    var queue = MessageQueue.new(3);
    try queue.prepend(&m1);

    try std.testing.expectEqual(1, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(queue.tail, &m1);

    try queue.prepend(&m2);

    try std.testing.expectEqual(2, queue.count);
    try std.testing.expectEqual(queue.head, &m2);
    try std.testing.expectEqual(queue.head.?.next.?, &m1);
    try std.testing.expectEqual(queue.tail, &m1);

    try queue.prepend(&m3);

    try std.testing.expectEqual(3, queue.count);
    try std.testing.expectEqual(queue.head, &m3);
    try std.testing.expectEqual(queue.tail, &m1);

    try std.testing.expectError(QueueError.QueueFull, queue.enqueue(&m4));
}

test "enqueueMany/dequeueMany" {
    var m1 = Message.new();
    var m2 = Message.new();
    var m3 = Message.new();
    var m4 = Message.new();

    const messages1 = [_]*Message{ &m1, &m2, &m3 };

    var queue = MessageQueue.new(3);
    try queue.enqueueMany(&messages1);

    try std.testing.expectEqual(3, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(m2.next, &m3);
    try std.testing.expectEqual(queue.tail, &m3);

    const messages2 = [_]*Message{&m4};
    try std.testing.expectError(QueueError.QueueFull, queue.enqueueMany(&messages2));

    try std.testing.expectEqual(3, queue.count);

    var buf: [2]*Message = undefined;
    const n = queue.dequeueMany(&buf);

    try std.testing.expectEqual(n, buf.len);
    try std.testing.expectEqual(buf[0], &m1);
    try std.testing.expectEqual(buf[1], &m2);

    try std.testing.expectEqual(1, queue.count);
    try std.testing.expectEqual(queue.head, &m3);
    try std.testing.expectEqual(queue.tail, &m3);
}

test "remove" {
    var m1 = Message.new();
    var m2 = Message.new();
    var m3 = Message.new();
    var m4 = Message.new();

    const messages1 = [_]*Message{ &m1, &m2, &m3 };

    var queue = MessageQueue.new(3);

    try std.testing.expectError(QueueError.QueueEmpty, queue.remove(&m1));

    try queue.enqueueMany(&messages1);

    try std.testing.expectError(QueueError.NotFound, queue.remove(&m4));

    try std.testing.expectEqual(3, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(queue.tail, &m3);

    try std.testing.expectEqual(&m2, m1.next);
    try std.testing.expectEqual(&m3, m2.next);
    try std.testing.expectEqual(null, m3.next);

    try queue.remove(&m1);

    try std.testing.expectEqual(2, queue.count);
    try std.testing.expectEqual(&m2, queue.head);
    try std.testing.expectEqual(&m3, queue.tail);

    try std.testing.expectEqual(null, m1.next);
    try std.testing.expectEqual(&m3, m2.next);
    try std.testing.expectEqual(null, m3.next);

    try queue.remove(&m3);

    try std.testing.expectEqual(1, queue.count);
    try std.testing.expectEqual(&m2, queue.head);
    try std.testing.expectEqual(&m2, queue.tail);

    try std.testing.expectEqual(null, m1.next);
    try std.testing.expectEqual(null, m2.next);
    try std.testing.expectEqual(null, m3.next);

    try queue.remove(&m2);

    try std.testing.expectEqual(0, queue.count);
    try std.testing.expectEqual(null, queue.head);
    try std.testing.expectEqual(null, queue.tail);

    try std.testing.expectEqual(null, m1.next);
    try std.testing.expectEqual(null, m2.next);
    try std.testing.expectEqual(null, m3.next);

    try queue.enqueueMany(&messages1);

    try queue.remove(&m3);

    try std.testing.expectEqual(2, queue.count);
    try std.testing.expectEqual(&m1, queue.head);
    try std.testing.expectEqual(&m2, queue.tail);

    try std.testing.expectEqual(&m2, m1.next);
    try std.testing.expectEqual(null, m2.next);
    try std.testing.expectEqual(null, m3.next);
}

test "reset" {
    var m1 = Message.new();
    var m2 = Message.new();
    var m3 = Message.new();

    const messages1 = [_]*Message{ &m1, &m2, &m3 };

    var queue = MessageQueue.new(3);
    try queue.enqueueMany(&messages1);

    try std.testing.expectEqual(3, queue.count);
    try std.testing.expectEqual(queue.head, &m1);
    try std.testing.expectEqual(queue.tail, &m3);

    try std.testing.expectEqual(&m2, m1.next);
    try std.testing.expectEqual(&m3, m2.next);
    try std.testing.expectEqual(null, m3.next);

    queue.reset();

    try std.testing.expectEqual(0, queue.count);
    try std.testing.expectEqual(null, queue.head);
    try std.testing.expectEqual(null, queue.tail);

    try std.testing.expectEqual(null, m1.next);
    try std.testing.expectEqual(null, m2.next);
    try std.testing.expectEqual(null, m3.next);
}
