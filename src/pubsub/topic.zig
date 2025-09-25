const std = @import("std");
const log = std.log.scoped(.Topic);
const testing = std.testing;
const assert = std.debug.assert;
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message2.zig").Message;
const Envelope = @import("../node/envelope.zig").Envelope;

const Subscriber = @import("./subscriber.zig").Subscriber;

pub const TopicOptions = struct {
    queue_capacity: usize = constants.topic_max_queue_capacity,
    subscriber_queue_capacity: usize = constants.subscriber_max_queue_capacity,
};

pub const TopicError = error{
    TopicQueueFull,
    UnableToProcess,
};

pub const Topic = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    memory_pool: *MemoryPool(Message),
    // publishers: std.AutoHashMap(u128, *Publisher),
    queue: *RingBuffer(Envelope),
    subscriber_queues: std.ArrayList(*RingBuffer(Envelope)),
    subscribers: std.AutoHashMapUnmanaged(u64, *Subscriber),
    topic_name: []const u8,
    tmp_copy_buffer: []Envelope,

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        topic_name: []const u8,
        options: TopicOptions,
    ) !Self {
        const queue = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(queue);

        // TODO: the buffer size should be configured. perhaps this could be a NodeConfig thing
        queue.* = try RingBuffer(Envelope).init(allocator, options.queue_capacity);
        errdefer queue.deinit();

        const tmp_copy_buffer = try allocator.alloc(Envelope, options.subscriber_queue_capacity);
        errdefer allocator.free(tmp_copy_buffer);

        return Self{
            .allocator = allocator,
            .memory_pool = memory_pool,
            .queue = queue,
            .subscriber_queues = .empty,
            .subscribers = .empty,
            .topic_name = topic_name,
            .tmp_copy_buffer = tmp_copy_buffer,
        };
    }

    pub fn deinit(self: *Self) void {
        self.clearQueue();

        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |entry| {
            const subscriber = entry.*;

            while (subscriber.queue.dequeue()) |envelope| {
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            subscriber.deinit();
            self.allocator.destroy(subscriber);
        }

        self.queue.deinit();
        self.subscriber_queues.deinit(self.allocator);
        self.subscribers.deinit(self.allocator);

        self.allocator.destroy(self.queue);
        self.allocator.free(self.tmp_copy_buffer);
    }

    pub fn tick(self: *Self) !void {
        // log.info("topic subscriers: {}, queue: {}", .{ self.subscribers.count(), self.queue.count });
        // There are no messages needing to be distributed to subscribers
        if (self.queue.count == 0) return;

        // there are no subscribers who are able to consume this message. We should not hang on to these messages
        if (self.subscribers.count() == 0) {
            self.clearQueue();
            return;
        }

        if (self.subscriber_queues.items.len != self.subscribers.count()) {
            try self.subscriber_queues.resize(self.allocator, self.subscribers.count());
        }

        // The subscriber queues.items.len should always be equal to the number of subscribers and should be updated
        // whenever a subscriber `subscribe`s to the this topic or `unsubscribe`s from this topic
        assert(self.subscriber_queues.items.len == self.subscribers.count());

        var i: usize = 0;
        var subscribers_iter = self.subscribers.valueIterator();
        while (subscribers_iter.next()) |subscriber_entry| : (i += 1) {
            const subscriber = subscriber_entry.*;
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
        for (self.tmp_copy_buffer[0..n]) |envelope| {
            // increase the number of refs for this message to match how many subscribers
            // the message will be added to
            _ = envelope.message.ref_count.fetchAdd(@intCast(self.subscriber_queues.items.len), .seq_cst);

            // deref once for the bus since it is giving up control
            envelope.message.deref();
        }

        for (self.subscriber_queues.items) |queue| {
            const x = queue.enqueueMany(self.tmp_copy_buffer[0..n]);
            assert(x == n);
        }
    }

    pub fn addSubscriber(self: *Self, subscriber_key: u64, session_id: u64) !void {
        if (self.subscribers.contains(subscriber_key)) return error.AlreadyExists;

        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = try Subscriber.init(
            self.allocator,
            subscriber_key,
            session_id,
            constants.subscriber_max_queue_capacity,
        );
        errdefer subscriber.deinit();

        try self.subscribers.put(self.allocator, subscriber_key, subscriber);
    }

    pub fn removeSubscriber(self: *Self, subscriber_key: u64) bool {
        if (self.subscribers.fetchRemove(subscriber_key)) |entry| {
            const subscriber = entry.value;

            while (subscriber.queue.dequeue()) |envelope| {
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }

            subscriber.deinit();
            self.allocator.destroy(subscriber);

            return true;
        }

        return false;
    }

    fn clearQueue(self: *Self) void {
        while (self.queue.dequeue()) |envelope| {
            assert(envelope.message.refs() == 1);

            envelope.message.deref();
            if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
        }
    }
};
