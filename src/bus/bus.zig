const std = @import("std");
const uuid = @import("uuid");

const log = std.log.scoped(.Bus);
const testing = std.testing;
const assert = std.debug.assert;

const RingBuffer = @import("stdx").RingBuffer;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const Producer = @import("./producer.zig").Producer;
const Consumer = @import("./consumer.zig").Consumer;

const BUS_QUEUE_SIZE = 1_000;

pub fn Bus(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex,
        producers_mutex: std.Thread.Mutex,
        consumers_mutex: std.Thread.Mutex,
        queue: *RingBuffer(T),
        consumers: *std.ArrayList(*Consumer(T)),
        producers: *std.ArrayList(*Producer(T)),
        close_channel: UnbufferedChannel(bool),
        last_producer_index: usize,
        topic_name: []const u8,

        pub fn init(allocator: std.mem.Allocator, topic_name: []const u8, queue_capacity: usize) !Self {
            const queue = try allocator.create(RingBuffer(T));
            errdefer allocator.destroy(queue);

            queue.* = try RingBuffer(T).init(allocator, queue_capacity);
            errdefer queue.deinit();

            const consumers = try allocator.create(std.ArrayList(*Consumer(T)));
            errdefer allocator.destroy(consumers);

            consumers.* = std.ArrayList(*Consumer(T)).init(allocator);
            errdefer consumers.deinit();

            const producers = try allocator.create(std.ArrayList(*Producer(T)));
            errdefer allocator.destroy(producers);

            producers.* = std.ArrayList(*Producer(T)).init(allocator);
            errdefer producers.deinit();

            return Self{
                .allocator = allocator,
                .mutex = std.Thread.Mutex{},
                .queue = queue,
                .consumers = consumers,
                .producers = producers,
                .close_channel = UnbufferedChannel(bool).new(),
                .last_producer_index = 0,
                .producers_mutex = std.Thread.Mutex{},
                .consumers_mutex = std.Thread.Mutex{},
                .topic_name = topic_name,
            };
        }

        pub fn deinit(self: *Self) void {
            self.queue.deinit();
            self.consumers.deinit();
            self.producers.deinit();

            self.allocator.destroy(self.queue);
            self.allocator.destroy(self.consumers);
            self.allocator.destroy(self.producers);
        }

        pub fn tick(self: *Self) !void {
            if (self.producers.items.len > 0) {
                self.mutex.lock();
                defer self.mutex.unlock();

                var processed: usize = 0;
                const producer_count = self.producers.items.len;

                while (processed < producer_count) : (processed += 1) {
                    const producer_index = (self.last_producer_index + processed) % producer_count;
                    const producer = self.producers.items[producer_index];

                    if (self.queue.available() == 0) {
                        log.debug("bus queue full producer index: {}", .{producer_index});

                        // next tick should resume from the next producer
                        self.last_producer_index = producer_index;
                        break;
                    }

                    producer.mutex.lock();
                    defer producer.mutex.unlock();

                    _ = self.queue.concatenateAvailable(producer.queue);
                }

                // If we completed the loop, set last index to the next producer
                if (processed == producer_count and self.queue.available() > 0) {
                    self.last_producer_index = (self.last_producer_index + 1) % producer_count;
                }
            }

            // if there are no items in the queue, then there is nothing to do
            if (self.queue.count == 0) return;

            // if there are no consumers of the items on the bus, then there is no work to be done
            if (self.consumers.items.len == 0) return;

            if (self.consumers.items.len > 0) {
                // Ensure that the consumers array list does not change
                self.consumers_mutex.lock();
                defer self.consumers_mutex.unlock();

                // TODO: move this to to `Bus.consumer_queues` so we don't have to reallocate every iteration.
                const consumer_queues = try self.allocator.alloc(
                    *RingBuffer(T),
                    self.consumers.items.len,
                );
                defer self.allocator.free(consumer_queues);

                self.mutex.lock();
                defer self.mutex.unlock();

                var max_available = self.queue.count;
                for (self.consumers.items) |consumer| {
                    const consumer_available = consumer.queue.available();
                    if (consumer_available < max_available) {
                        max_available = consumer_available;
                    }
                }

                // lock this individual consumer so that we can copy stuff to their queue
                for (self.consumers.items, 0..self.consumers.items.len) |consumer, i| {
                    consumer.mutex.lock();
                    consumer_queues[i] = consumer.queue;
                }
                defer {
                    for (self.consumers.items) |consumer| {
                        consumer.mutex.unlock();
                    }
                }

                _ = self.queue.copyMaxToOthers(consumer_queues);
            }
        }

        pub fn addConsumer(self: *Self, consumer: *Consumer(T)) !void {
            self.consumers_mutex.lock();
            defer self.consumers_mutex.unlock();

            for (self.consumers.items) |existing_consumer| {
                if (existing_consumer.id == consumer.id) return error.ConsumerExists;
            }

            try self.consumers.append(consumer);
        }

        pub fn addProducer(self: *Self, producer: *Producer(T)) !void {
            self.producers_mutex.lock();
            defer self.producers_mutex.unlock();

            for (self.producers.items) |existing_producer| {
                if (existing_producer.id == producer.id) return error.ProducerExists;
            }

            try self.producers.append(producer);
        }

        pub fn removeConsumer(self: *Self, consumer_id: uuid.Uuid) bool {
            self.consumers_mutex.lock();
            defer self.consumers_mutex.unlock();

            for (self.consumers.items, 0..self.consumers.items.len) |existing_consumer, i| {
                if (existing_consumer.id == consumer_id) return self.consumers.swapRemove(i);
            }
        }

        pub fn removeProducer(self: *Self, producer_id: uuid.Uuid) bool {
            self.producers_mutex.lock();
            defer self.producers_mutex.unlock();

            for (self.producers.items, 0..self.producers.items.len) |existing_producer, i| {
                if (existing_producer.id == producer_id) return self.producers.swapRemove(i);
            }
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
    const producer_queue_capacity = 800;
    const consumer_queue_capacity = 500;

    var bus = try Bus(u32).init(allocator, topic_name, bus_queue_capacity);
    defer bus.deinit();

    var producer = try Producer(u32).init(allocator, 1, uuid.v7.new(), producer_queue_capacity);
    defer producer.deinit();

    var consumer = try Consumer(u32).init(allocator, 1, uuid.v7.new(), consumer_queue_capacity);
    defer consumer.deinit();

    try bus.addConsumer(&consumer);
    try bus.addProducer(&producer);

    try testing.expectEqual(0, producer.queue.count);
    try testing.expectEqual(0, consumer.queue.count);

    try producer.produce(1);

    try testing.expectEqual(1, producer.queue.count);
    try testing.expectEqual(0, consumer.queue.count);

    try bus.tick();

    try testing.expectEqual(0, producer.queue.count);
    try testing.expectEqual(1, consumer.queue.count);

    // NOTE: simulate the consumer processing the value
    consumer.queue.reset();

    for (0..producer.queue.capacity) |_| {
        try producer.produce(1);
    }

    try bus.tick();

    try testing.expectEqual(producer_queue_capacity - consumer_queue_capacity, bus.queue.count);
}
