const std = @import("std");
const zbench = @import("zbench");

const assert = std.debug.assert;
const constants = @import("./constants.zig");
const testing = std.testing;

const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const Message = @import("../protocol/message.zig").Message;

const BenchmarkMessageQueueEnqueue = struct {
    const Self = @This();

    list: *std.ArrayList(*Message),
    message_queue: *MessageQueue,

    fn new(list: *std.ArrayList(*Message), message_queue: *MessageQueue) Self {
        return .{
            .list = list,
            .message_queue = message_queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        // enqueue every data point in the list into the ring buffer
        for (self.list.items) |data| {
            self.message_queue.enqueue(data);
        }

        // drop ALL references immediately
        self.message_queue.reset();
    }
};

const BenchmarkMessageQueueEnqueueMany = struct {
    const Self = @This();

    list: *std.ArrayList(*Message),
    message_queue: *MessageQueue,

    fn new(list: *std.ArrayList(*Message), message_queue: *MessageQueue) Self {
        return .{
            .list = list,
            .message_queue = message_queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        // enqueue every data point in the list into the ring buffer
        self.message_queue.enqueueMany(self.list.items);

        assert(self.message_queue.count == self.list.items.len);

        // drop ALL references immediately
        self.message_queue.reset();
    }
};

const BenchmarkMessageQueueDequeue = struct {
    const Self = @This();

    list: *std.ArrayList(*Message),
    message_queue: *MessageQueue,

    fn new(list: *std.ArrayList(*Message), message_queue: *MessageQueue) Self {
        return .{
            .list = list,
            .message_queue = message_queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        while (self.message_queue.dequeue()) |_| {}
        assert(self.message_queue.isEmpty());
    }
};

const BenchmarkMessageQueueDequeueMany = struct {
    const Self = @This();

    list: *std.ArrayList(*Message),
    message_queue: *MessageQueue,

    fn new(list: *std.ArrayList(*Message), message_queue: *MessageQueue) Self {
        return .{
            .list = list,
            .message_queue = message_queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        const n = self.message_queue.dequeueMany(self.list.items);

        assert(self.message_queue.isEmpty());
        assert(n == self.list.items.len);
    }
};

var message_queue_enqueue: MessageQueue = undefined;
var message_queue_enqueueMany: MessageQueue = undefined;
var message_queue_dequeue: MessageQueue = undefined;
var message_queue_dequeueMany: MessageQueue = undefined;
var data_list: std.ArrayList(*Message) = undefined;
const allocator = testing.allocator;

fn beforeEachDequeue() void {
    message_queue_dequeue.enqueueMany(data_list.items);

    // this is a sanity check
    assert(message_queue_dequeue.count == data_list.items.len);
}

fn beforeEachDequeueMany() void {
    message_queue_dequeueMany.enqueueMany(data_list.items);

    // this is a sanity check
    assert(message_queue_dequeueMany.count == data_list.items.len);
}

test "MessageQueue benchmarks" {
    var bench = zbench.Benchmark.init(
        std.testing.allocator,
        .{ .iterations = constants.benchmark_max_iterations },
    );
    defer bench.deinit();

    // Create a list of `n` length that will be used/reused by each benchmarking test
    data_list = try std.ArrayList(*Message).initCapacity(
        allocator,
        constants.benchmark_max_queue_data_list,
    );
    defer data_list.deinit();

    // fill the data list with items
    for (0..data_list.capacity) |i| {
        const message = try allocator.create(Message);
        errdefer allocator.destroy(message);

        message.* = Message.new();
        message.headers.message_type = .ping;
        message.setTransactionId(@intCast(i));

        data_list.appendAssumeCapacity(message);
    }

    // Initialize all ring buffers used in benchmarks
    message_queue_enqueue = MessageQueue.new();
    message_queue_enqueueMany = MessageQueue.new();
    message_queue_dequeue = MessageQueue.new();
    message_queue_dequeueMany = MessageQueue.new();

    const message_queue_enqueue_title = try std.fmt.allocPrint(
        allocator,
        "enqueue {} items",
        .{constants.benchmark_max_queue_data_list},
    );
    defer allocator.free(message_queue_enqueue_title);

    const message_queue_enqueueMany_title = try std.fmt.allocPrint(
        allocator,
        "enqueueMany {} items",
        .{constants.benchmark_max_queue_data_list},
    );
    defer allocator.free(message_queue_enqueueMany_title);

    const message_queue_dequeue_title = try std.fmt.allocPrint(
        allocator,
        "dequeue {} items",
        .{constants.benchmark_max_queue_data_list},
    );
    defer allocator.free(message_queue_dequeue_title);

    const message_queue_dequeueMany_title = try std.fmt.allocPrint(
        allocator,
        "dequeueMany {} items",
        .{constants.benchmark_max_queue_data_list},
    );
    defer allocator.free(message_queue_dequeueMany_title);

    // register all the benchmark tests
    try bench.addParam(
        message_queue_enqueue_title,
        &BenchmarkMessageQueueEnqueue.new(&data_list, &message_queue_enqueue),
        .{},
    );
    try bench.addParam(
        message_queue_enqueueMany_title,
        &BenchmarkMessageQueueEnqueueMany.new(&data_list, &message_queue_enqueueMany),
        .{},
    );
    try bench.addParam(
        message_queue_dequeue_title,
        &BenchmarkMessageQueueDequeue.new(&data_list, &message_queue_dequeue),
        .{
            .hooks = .{
                .before_each = beforeEachDequeue,
            },
        },
    );
    try bench.addParam(
        message_queue_dequeueMany_title,
        &BenchmarkMessageQueueDequeueMany.new(&data_list, &message_queue_dequeueMany),
        .{
            .hooks = .{
                .before_each = beforeEachDequeueMany,
            },
        },
    );

    // Write the results to stderr
    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("\n");
    try stderr.writeAll("|-------------------------|\n");
    try stderr.writeAll("| MessageQueue Benchmarks |\n");
    try stderr.writeAll("|-------------------------|\n");
    try bench.run(stderr);
}
