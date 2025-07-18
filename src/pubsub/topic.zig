const std = @import("std");
const log = std.log.scoped(.Topic);
const testing = std.testing;
const assert = std.debug.assert;
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const Message = @import("../protocol/message.zig").Message;

const Publisher = @import("./publisher.zig").Publisher;
const Subscriber = @import("./subscriber.zig").Subscriber;

pub const TopicOptions = struct {};

pub const Topic = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    memory_pool: *MemoryPool(Message),
    publishers: std.AutoHashMap(u128, *Publisher),
    queue: *RingBuffer(*Message),
    subscriber_queues: std.ArrayList(*RingBuffer(*Message)),
    subscribers: std.AutoHashMap(u128, *Subscriber),
    topic_name: []const u8,
    tmp_copy_buffer: []*Message,

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        topic_name: []const u8,
    ) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        // TODO: the buffer size should be configured. perhaps this could be a NodeConfig thing
        queue.* = try RingBuffer(*Message).init(allocator, constants.topic_max_queue_capacity);
        errdefer queue.deinit();

        const tmp_copy_buffer = try allocator.alloc(*Message, constants.subscriber_max_queue_capacity);
        errdefer allocator.free(tmp_copy_buffer);

        return Self{
            .allocator = allocator,
            .memory_pool = memory_pool,
            .publishers = std.AutoHashMap(u128, *Publisher).init(allocator),
            .queue = queue,
            .subscriber_queues = std.ArrayList(*RingBuffer(*Message)).init(allocator),
            .subscribers = std.AutoHashMap(u128, *Subscriber).init(allocator),
            .topic_name = topic_name,
            .tmp_copy_buffer = tmp_copy_buffer,
        };
    }

    pub fn deinit(self: *Self) void {
        assert(self.queue.count == 0);
        self.clearQueue();

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;
            self.allocator.destroy(subscriber);
        }

        var publishers_iter = self.publishers.valueIterator();
        while (publishers_iter.next()) |entry| {
            const publisher = entry.*;
            self.allocator.destroy(publisher);
        }

        self.subscribers.deinit();
        self.publishers.deinit();
        self.queue.deinit();
        self.subscriber_queues.deinit();

        self.allocator.destroy(self.queue);
        self.allocator.free(self.tmp_copy_buffer);
    }

    pub fn enqueue(self: *Self, message: *Message) !void {
        self.queue.enqueue(message) catch |err| {
            // log.err("topic unable to enqueue message: {s}, err: {any}", .{ self.topic_name, err });
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
            return err;
        };

        message.ref();
    }

    pub fn tick(self: *Self) !void {
        // There are no messages needing to be processed
        if (self.queue.count == 0) return;
        // there are no subscribers who are able to consume this message. We should not hang on to these messages
        if (self.subscribers.count() == 0) {
            self.clearQueue();
            return;
        }

        if (self.subscriber_queues.items.len != self.subscribers.count()) {
            try self.subscriber_queues.resize(self.subscribers.count());
        }

        // The subscriber queues.items.len should always be equal to the number of subscribers and should be updated
        // whenever a subscriber `subscribe`s to the this topic or `unsubscribe`s from this topic
        assert(self.subscriber_queues.items.len == self.subscribers.count());

        var i: usize = 0;
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| : (i += 1) {
            const subscriber = entry.*;
            self.subscriber_queues.items[i] = subscriber.queue;
        }

        // FIX: this can be done in a much cleaner way. This basically requires multiple loops
        var max_copy = self.queue.count;
        for (self.subscriber_queues.items) |queue| {
            if (queue.available() < max_copy) {
                max_copy = queue.available();
            }
        }

        if (max_copy == 0) return;

        const n = self.queue.dequeueMany(self.tmp_copy_buffer[0..max_copy]);
        for (self.tmp_copy_buffer[0..n]) |message| {
            // increase the number of refs for this message to match how many subscribers
            // the message will be added to
            _ = message.ref_count.fetchAdd(@intCast(self.subscriber_queues.items.len), .seq_cst);

            // deref once for the bus since it is giving up control
            message.deref();
        }

        for (self.subscriber_queues.items) |queue| {
            const x = queue.enqueueMany(self.tmp_copy_buffer[0..n]);
            assert(x == n);
        }
    }

    fn clearQueue(self: *Self) void {
        while (self.queue.dequeue()) |message| {
            assert(message.refs() == 1);
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }
    }
};
