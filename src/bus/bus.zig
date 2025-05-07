const std = @import("std");
const uuid = @import("uuid");

const log = std.log.scoped(.Bus);
const testing = std.testing;
const assert = std.debug.assert;

const RingBuffer = @import("stdx").RingBuffer;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const Publisher = @import("./publisher.zig").Publisher;
const Subscriber = @import("./subscriber.zig").Subscriber;

pub fn Bus(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex,
        publishers_mutex: std.Thread.Mutex,
        subscribers_mutex: std.Thread.Mutex,
        queue: *RingBuffer(T),
        subscribers: *std.ArrayList(*Subscriber(T)),
        publishers: *std.ArrayList(*Publisher(T)),
        close_channel: UnbufferedChannel(bool),
        last_publisher_index: usize,
        topic_name: []const u8,

        pub fn init(allocator: std.mem.Allocator, topic_name: []const u8, queue_capacity: usize) !Self {
            const queue = try allocator.create(RingBuffer(T));
            errdefer allocator.destroy(queue);

            queue.* = try RingBuffer(T).init(allocator, queue_capacity);
            errdefer queue.deinit();

            const subscribers = try allocator.create(std.ArrayList(*Subscriber(T)));
            errdefer allocator.destroy(subscribers);

            subscribers.* = std.ArrayList(*Subscriber(T)).init(allocator);
            errdefer subscribers.deinit();

            const publishers = try allocator.create(std.ArrayList(*Publisher(T)));
            errdefer allocator.destroy(publishers);

            publishers.* = std.ArrayList(*Publisher(T)).init(allocator);
            errdefer publishers.deinit();

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
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
            self.subscribers.deinit();
            self.publishers.deinit();

            self.allocator.destroy(self.queue);
            self.allocator.destroy(self.subscribers);
            self.allocator.destroy(self.publishers);
        }

        pub fn tick(self: *Self) !void {
            if (self.subscribers.items.len > 0) {
                // Ensure that the subscribers array list does not change
                self.subscribers_mutex.lock();
                defer self.subscribers_mutex.unlock();

                // TODO: move this to to `Bus.subscriber_queues` so we don't have to reallocate every iteration.
                const subscriber_queues = try self.allocator.alloc(
                    *RingBuffer(T),
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

                // each item needs to be refed for `n` subscribers
                _ = self.queue.copyMaxToOthers(subscriber_queues);
            }

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

        pub fn addSubscriber(self: *Self, subscriber: *Subscriber(T)) !void {
            self.subscribers_mutex.lock();
            defer self.subscribers_mutex.unlock();

            for (self.subscribers.items) |existing_subscriber| {
                if (existing_subscriber.key == subscriber.key) return error.SubscriberExists;
            }

            try self.subscribers.append(subscriber);
        }

        pub fn addPublisher(self: *Self, publisher: *Publisher(T)) !void {
            self.publishers_mutex.lock();
            defer self.publishers_mutex.unlock();

            for (self.publishers.items) |existing_publisher| {
                if (existing_publisher.key == publisher.key) return error.PublisherExists;
            }

            try self.publishers.append(publisher);
        }

        pub fn removeSubscriber(self: *Self, subscriber_key: u128) !*Subscriber(T) {
            self.subscribers_mutex.lock();
            defer self.subscribers_mutex.unlock();

            for (self.subscribers.items, 0..self.subscribers.items.len) |existing_subscriber, i| {
                if (existing_subscriber.key == subscriber_key) return self.subscribers.swapRemove(i);
            }

            return error.SubscriberDoesNotExist;
        }

        pub fn removePublisher(self: *Self, publisher_key: u128) !*Publisher(T) {
            self.publishers_mutex.lock();
            defer self.publishers_mutex.unlock();

            for (self.publishers.items, 0..self.publishers.items.len) |existing_publisher, i| {
                if (existing_publisher.key == publisher_key) return self.publishers.swapRemove(i);
            }

            return error.PublisherDoesNotExist;
        }
    };
}

test "init/deinit" {
    const allocator = testing.allocator;

    const topic_name = "/test/topic";

    var bus = try Bus(u32).init(allocator, topic_name, 1);
    defer bus.deinit();
}

test "tick" {
    const allocator = testing.allocator;

    const topic_name = "/test/topic";
    const bus_queue_capacity = 1_000;
    const publisher_queue_capacity = 800;
    const subscriber_queue_capacity = 500;

    var bus = try Bus(u32).init(allocator, topic_name, bus_queue_capacity);
    defer bus.deinit();

    var publisher = try Publisher(u32).init(allocator, 1, uuid.v7.new(), publisher_queue_capacity);
    defer publisher.deinit();

    var subscriber = try Subscriber(u32).init(allocator, 1, uuid.v7.new(), subscriber_queue_capacity, &bus);
    defer subscriber.deinit();

    try bus.addSubscriber(&subscriber);
    try bus.addPublisher(&publisher);

    try testing.expectEqual(0, publisher.queue.count);
    try testing.expectEqual(0, subscriber.queue.count);

    try publisher.publish(1);

    try testing.expectEqual(1, publisher.queue.count);
    try testing.expectEqual(0, subscriber.queue.count);

    try bus.tick();

    try testing.expectEqual(0, publisher.queue.count);
    try testing.expectEqual(1, subscriber.queue.count);

    // NOTE: simulate the subscriber processing the value
    subscriber.queue.reset();

    for (0..publisher.queue.capacity) |_| {
        try publisher.publish(1);
    }

    try bus.tick();

    try testing.expectEqual(publisher_queue_capacity - subscriber_queue_capacity, bus.queue.count);
}
