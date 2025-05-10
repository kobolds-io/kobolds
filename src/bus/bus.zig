const std = @import("std");
const uuid = @import("uuid");

const log = std.log.scoped(.Bus);
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const Publisher = @import("./publisher.zig").Publisher;
const Subscriber = @import("./subscriber.zig").Subscriber;

const Message = @import("../protocol/message.zig").Message;

pub const Bus = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    publishers_mutex: std.Thread.Mutex,
    subscribers_mutex: std.Thread.Mutex,
    queue: *RingBuffer(*Message),
    subscribers: *std.ArrayList(*Subscriber),
    publishers: *std.ArrayList(*Publisher),
    close_channel: UnbufferedChannel(bool),
    last_publisher_index: usize,
    topic_name: []const u8,
    messages_buffer: []*Message,

    pub fn init(allocator: std.mem.Allocator, topic_name: []const u8, queue_capacity: usize) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        queue.* = try RingBuffer(*Message).init(allocator, queue_capacity);
        errdefer queue.deinit();

        const subscribers = try allocator.create(std.ArrayList(*Subscriber));
        errdefer allocator.destroy(subscribers);

        subscribers.* = std.ArrayList(*Subscriber).init(allocator);
        errdefer subscribers.deinit();

        const publishers = try allocator.create(std.ArrayList(*Publisher));
        errdefer allocator.destroy(publishers);

        publishers.* = std.ArrayList(*Publisher).init(allocator);
        errdefer publishers.deinit();

        const messages_buffer = try allocator.alloc(*Message, constants.subscriber_max_queue_capacity);
        errdefer allocator.free(messages_buffer);

        return Self{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .queue = queue,
            .subscribers = subscribers,
            .publishers = publishers,
            .close_channel = UnbufferedChannel(bool).new(),
            .last_publisher_index = 0,
            .publishers_mutex = std.Thread.Mutex{},
            .subscribers_mutex = std.Thread.Mutex{},
            .topic_name = topic_name,
            .messages_buffer = messages_buffer,
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();
        self.subscribers.deinit();
        self.publishers.deinit();

        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.subscribers);
        self.allocator.destroy(self.publishers);
        self.allocator.free(self.messages_buffer);
    }

    pub fn tick(self: *Self) !void {
        self.gather();
        try self.distribute();
    }

    fn gather(self: *Self) void {
        if (self.publishers.items.len > 0) {
            self.mutex.lock();
            defer self.mutex.unlock();

            var processed: usize = 0;
            const publisher_count = self.publishers.items.len;

            while (processed < publisher_count) : (processed += 1) {
                const publisher_index = (self.last_publisher_index + processed) % publisher_count;
                const publisher = self.publishers.items[publisher_index];

                if (self.queue.available() == 0) {
                    log.debug("bus queue full publisher index: {}", .{publisher_index});

                    // next tick should resume from the next publisher
                    self.last_publisher_index = publisher_index;
                    break;
                }

                publisher.mutex.lock();
                defer publisher.mutex.unlock();

                self.queue.concatenateAvailable(publisher.queue);
            }

            // If we completed the loop, set last index to the next publisher
            if (processed == publisher_count and self.queue.available() > 0) {
                self.last_publisher_index = (self.last_publisher_index + 1) % publisher_count;
            }
        }
    }

    fn distribute(self: *Self) !void {
        if (self.subscribers.items.len > 0) {
            // Ensure that the subscribers array list does not change
            self.subscribers_mutex.lock();
            defer self.subscribers_mutex.unlock();

            // TODO: move this to to `Bus.subscriber_queues` so we don't have to reallocate every iteration.
            const subscriber_queues = try self.allocator.alloc(
                *RingBuffer(*Message),
                self.subscribers.items.len,
            );
            defer self.allocator.free(subscriber_queues);

            self.mutex.lock();
            defer self.mutex.unlock();

            // lock this individual subscriber so that we can copy stuff to their queue
            for (self.subscribers.items, 0..self.subscribers.items.len) |subscriber, i| {
                subscriber.mutex.lock();
                subscriber_queues[i] = subscriber.queue;
            }
            defer {
                for (self.subscribers.items) |subscriber| {
                    subscriber.mutex.unlock();
                }
            }

            // FIX: this can be done in a much cleaner way. This basically requires multiple loops
            var max_copy = self.queue.count;
            for (subscriber_queues) |queue| {
                if (queue.available() < max_copy) {
                    max_copy = queue.available();
                }
            }

            if (max_copy == 0) return;

            const n = self.queue.dequeueMany(self.messages_buffer[0..max_copy]);
            for (self.messages_buffer[0..n]) |message| {
                assert(message.refs() == 1);
                // increase the number of refs for this message to match how many subscribers
                // the message will be added to
                _ = message.ref_count.fetchAdd(@intCast(subscriber_queues.len), .seq_cst);

                // deref once for the bus since it is giving up control
                message.deref();
            }

            for (subscriber_queues) |queue| {
                const x = queue.enqueueMany(self.messages_buffer[0..n]);
                assert(x == n);
            }
        }
    }

    pub fn addSubscriber(self: *Self, subscriber: *Subscriber) !void {
        self.subscribers_mutex.lock();
        defer self.subscribers_mutex.unlock();

        for (self.subscribers.items) |existing_subscriber| {
            if (existing_subscriber.key == subscriber.key) return error.SubscriberExists;
        }

        try self.subscribers.append(subscriber);
    }

    pub fn addPublisher(self: *Self, publisher: *Publisher) !void {
        self.publishers_mutex.lock();
        defer self.publishers_mutex.unlock();

        for (self.publishers.items) |existing_publisher| {
            if (existing_publisher.key == publisher.key) return error.PublisherExists;
        }

        try self.publishers.append(publisher);
    }

    pub fn removeSubscriber(self: *Self, subscriber_key: u128) !*Subscriber {
        self.subscribers_mutex.lock();
        defer self.subscribers_mutex.unlock();

        for (self.subscribers.items, 0..self.subscribers.items.len) |existing_subscriber, i| {
            if (existing_subscriber.key == subscriber_key) return self.subscribers.swapRemove(i);
        }

        return error.SubscriberDoesNotExist;
    }

    pub fn removePublisher(self: *Self, publisher_key: u128) !*Publisher {
        self.publishers_mutex.lock();
        defer self.publishers_mutex.unlock();

        for (self.publishers.items, 0..self.publishers.items.len) |existing_publisher, i| {
            if (existing_publisher.key == publisher_key) return self.publishers.swapRemove(i);
        }

        return error.PublisherDoesNotExist;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    const topic_name = "/test/topic";

    var bus = try Bus.init(allocator, topic_name, 1);
    defer bus.deinit();
}

test "tick" {
    const allocator = testing.allocator;

    const topic_name = "/test/topic";
    const bus_queue_capacity = 1_000;
    const publisher_queue_capacity = 800;
    const subscriber_queue_capacity = 500;
    var message = Message.new();
    message.headers.message_type = .publish;

    var bus = try Bus.init(allocator, topic_name, bus_queue_capacity);
    defer bus.deinit();

    var publisher = try Publisher.init(allocator, 1, uuid.v7.new(), publisher_queue_capacity);
    defer publisher.deinit();

    var subscriber = try Subscriber.init(allocator, 1, uuid.v7.new(), subscriber_queue_capacity, &bus);
    defer subscriber.deinit();

    try bus.addSubscriber(&subscriber);
    try bus.addPublisher(&publisher);

    try testing.expectEqual(0, publisher.queue.count);
    try testing.expectEqual(0, subscriber.queue.count);

    try publisher.publish(&message);

    try testing.expectEqual(1, publisher.queue.count);
    try testing.expectEqual(0, subscriber.queue.count);

    try bus.tick();

    try testing.expectEqual(0, publisher.queue.count);
    try testing.expectEqual(1, subscriber.queue.count);

    // NOTE: simulate the subscriber processing the value
    subscriber.queue.reset();

    for (0..publisher.queue.capacity) |_| {
        try publisher.publish(&message);
    }

    try bus.tick();

    try testing.expectEqual(publisher_queue_capacity - subscriber_queue_capacity, bus.queue.count);
}
