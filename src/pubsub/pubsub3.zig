const std = @import("std");
const testing = std.testing;
const Message = @import("../protocol/message.zig").Message;
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const Topic = struct {
    const Self = @This();
    allocator: std.mem.Allocator,

    /// list of all messages
    message_map: std.AutoHashMap(*Message, std.mem.Allocator),
    unpublished_messages: RingBuffer(*Message),
    subscribers: std.AutoHashMap(*Subscriber, bool),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .message_map = std.AutoHashMap(*Message, std.mem.Allocator).init(allocator),
            .unpublished_messages = RingBuffer(*Message).init(allocator, 100) catch unreachable,
            .subscribers = std.AutoHashMap(*Subscriber, bool).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        var subs = self.subscribers.keyIterator();
        while (subs.next()) |entry| {
            const sub = entry.*;
            sub.deinit();

            self.allocator.destroy(sub);
        }

        // FIX: there has to be more complicated cleanup for this
        self.message_map.deinit();
        self.unpublished_messages.deinit();
        self.subscribers.deinit();
    }

    pub fn subscribe(self: *Self) !*Subscriber {
        const subscriber = try self.allocator.create(Subscriber);
        errdefer self.allocator.destroy(subscriber);

        subscriber.* = Subscriber.init(self.allocator, 1, self);
        errdefer subscriber.deinit();

        try self.subscribers.put(subscriber, true);
        return subscriber;
    }

    pub fn unsubscribe(self: *Self, subscriber_ptr: *Subscriber) void {
        if (self.subscribers.getKey(subscriber_ptr)) |sub| {
            sub.deinit();
            _ = self.subscribers.remove(subscriber_ptr);
            self.allocator.destroy(subscriber_ptr);
        }
    }

    pub fn tick(self: *Self) !void {
        try self.publish();
        try self.prune();
    }

    fn publish(self: *Self) !void {
        // perhaps we have a buffer that we keep on disk. For example, if we have 1_000_000 messages,
        // we might not want to keep all of those in memory because it is unrealistic that we would be able
        // to process them all at the same time. Perhaps past a certain limit, say 50_000 messages, we write new
        // messages to disk. On every tick we read work through what we have in memory and then refill our
        // in memory buffer with messages we read from disk. This might need to be a totally special allocator
        // that belongs to the topic and not the publisher. So slightly different but same pattern overall.

        while (self.unpublished_messages.dequeue()) |message| {
            var subs = self.subscribers.keyIterator();
            while (subs.next()) |entry| {
                const sub = entry.*;
                message.ref();

                // we would want to check how many messages are in the queue and if we are getting full,
                // then we should dequeue the oldest item and dereference it so it can be cleaned up later.
                // This would mean that some subscribers might be so slow that they cannot process all of the
                // messages and will only receive "some" messages.
                if (sub.messages.available() == 0) {
                    const dropped_message = sub.messages.dequeue().?;
                    dropped_message.deref();
                    // log an error here
                }
                try sub.messages.enqueue(message);
            }
        }
    }

    fn prune(self: *Self) !void {
        // clean out old dead messages from the topic
        var messages_iter = self.message_map.iterator();
        while (messages_iter.next()) |entry| {
            const message = entry.key_ptr.*;
            const message_allocator = entry.value_ptr.*;

            if (message.refs() > 0) continue;

            // there would need to be a lock on this destroy. So a threadsafe memory pool is still required
            // but this guarantees that I only ever need a single allocation for this message which is beneficial,
            // perhaps more beneficial than anything else.
            message_allocator.destroy(message);
            self.message_map.removeByPtr(entry.key_ptr);
        }
    }
};

const Publisher = struct {
    const Self = @This();
    allocator: std.mem.Allocator,
    connection: u32,
    topic: *Topic,

    pub fn init(allocator: std.mem.Allocator, connection: u32, topic: *Topic) Self {
        return Self{
            .allocator = allocator,
            .connection = connection,
            .topic = topic,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    pub fn publish(self: *Self, allocator: std.mem.Allocator, message: *Message) !void {
        self.topic.mutex.lock();
        defer self.topic.mutex.unlock();

        try self.topic.message_map.put(message, allocator);
        try self.topic.unpublished_messages.enqueue(message);
    }
};

const Subscriber = struct {
    const Self = @This();
    messages: RingBuffer(*Message),
    connection: u32,
    allocator: std.mem.Allocator,
    topic: *Topic,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, connection: u32, topic: *Topic) Self {
        return Self{
            .allocator = allocator,
            .connection = connection,
            .messages = RingBuffer(*Message).init(allocator, 100) catch unreachable,
            .topic = topic,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.messages.deinit();
    }

    pub fn gather(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // on the subscriber worker thread which is a subscriber.tick();
        if (self.messages.count > 0) {
            while (self.messages.dequeue()) |message_ref| {
                // this would be copying the messages queue to the
                message_ref.deref();
            }
        }
    }
};

const Node = struct {
    allocator: std.mem.Allocator,
    envelopes: RingBuffer(Envelope),
};
const Worker = struct {
    allocator: std.mem.Allocator,
    id: u32,
    envelopes: RingBuffer(Envelope),
};
const Connection = struct {};

const Envelope = struct {
    worker_id: u32,
    message: *Message,
    allocator: std.mem.Allocator,
};

// 1. Node init spawns all workers & tcp_acceptor
// 2. Worker init accepts connections and references the `node`.
// 3. Worker creates a message pool
// 4. Connection receives message
// 5. Connection uses the worker (thread) message pool to create new messages
// 6. Worker aggregates the messages from the connection
// ------ Handling topics
// 1. `worker` enqueues an `Envelope` the `node`s `unprocessed_messages_queue`.
//      1. Envelopes are pure value objects and shouldn't need to be allocated onto the heap
// 2. `node.tick()` processes the messages `unprocessed_messages_queue`.
//      1. if the message is a `publish`, the `node` will create an `Envelope` and pass it to every `subscriber`

test "passing messages" {
    const ITERATIONS = 5_000;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var message_pool = try MessagePool.init(allocator, ITERATIONS);
    defer message_pool.deinit();

    // const allocator = testing.allocator;

    // const s = try a.alloc(u8, (@sizeOf(Envelope) + @sizeOf(Message)) * ITERATIONS * 3);
    // defer a.free(s);
    // var fba = std.heap.FixedBufferAllocator.init(s);
    // const allocator = fba.allocator();

    var node = Node{
        .allocator = allocator,
        .envelopes = try RingBuffer(Envelope).init(allocator, ITERATIONS),
    };
    defer node.envelopes.deinit();

    // worker is running in a background thread...
    var worker_1 = Worker{
        .allocator = allocator,
        .id = 1,
        .envelopes = try RingBuffer(Envelope).init(allocator, ITERATIONS),
    };
    defer worker_1.envelopes.deinit();

    var worker_2 = Worker{
        .allocator = allocator,
        .id = 2,
        .envelopes = try RingBuffer(Envelope).init(allocator, ITERATIONS),
    };
    defer worker_2.envelopes.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    var start = timer.read();
    const total_start = start;
    var end = start;

    for (0..ITERATIONS) |i| {
        // Worker receives a message
        // const message = try gpa_allocator.create(Message);
        const message = try message_pool.create();

        message.* = Message.new();
        message.headers.message_type = .ping;
        message.setTransactionId(@intCast(i + 1));
        try testing.expectEqual(null, message.validate());

        // worker wraps the message into an envelope
        const envelope = Envelope{
            .message = message,
            .worker_id = worker_1.id,
            .allocator = allocator,
        };

        // try worker.messages.enqueue(message);
        try worker_1.envelopes.enqueue(envelope);
        // std.debug.print("worker {} env.message {*} - enqueue to worker\n", .{ worker_1.id, envelope.message });
    }
    end = timer.read();
    std.debug.print("took {}us to create & enqueue on worker\n", .{(end - start) / std.time.ns_per_us});

    start = timer.read();
    // worker pushes the envelopes to the node
    // FIX: Make this a concatenate
    while (worker_1.envelopes.dequeue()) |env| {
        // std.debug.print("worker {} env.message {*} - enqueue to node\n", .{ worker_1.id, env.message });
        try node.envelopes.enqueue(env);
    }
    end = timer.read();
    std.debug.print("took {}us to enqueue on node\n", .{(end - start) / std.time.ns_per_us});

    // node tick....
    // at some point the node has separated out the envelopes destined for the worker_2

    start = timer.read();
    // worker_2 takes the envelopes from the node
    while (node.envelopes.dequeue()) |env| {
        // std.debug.print("worker {} env.message {*} - enqueue to worker\n", .{ worker_2.id, env.message });
        try worker_2.envelopes.enqueue(env);
    }
    end = timer.read();
    std.debug.print("took {}us to dequeue from node and enqueue on worker\n", .{(end - start) / std.time.ns_per_us});

    start = timer.read();
    // worker_2 sends the message to the connection
    while (worker_2.envelopes.dequeue()) |env| {
        // std.debug.print("worker {} env.message {*} - cleanup\n", .{ worker_2.id, env.message });

        // clean up the message
        // env.allocator.destroy(env.message);
        message_pool.destroy(env.message);
    }

    end = timer.read();

    std.debug.print("took {}us destroy all messages\n", .{(end - start) / std.time.ns_per_us});
    std.debug.print("took {}us total\n", .{(end - total_start) / std.time.ns_per_us});
    // after some time the worker pushes the messages to the node's broker

}
