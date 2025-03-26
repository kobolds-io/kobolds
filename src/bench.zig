const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;
const testing = std.testing;

const Parser = @import("./protocol/parser.zig").Parser;
const Message = @import("./protocol/message.zig").Message;
const ManagedQueue = @import("./data_structures/managed_queue.zig").ManagedQueue;
const UnmanagedQueue = @import("./data_structures/unmanaged_queue.zig").UnmanagedQueue;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;
const RingBuffer = @import("./data_structures/ring_buffer.zig").RingBuffer;

const constants = @import("./constants.zig");

const ParserBenchmark = struct {
    messages: *std.ArrayList(Message),
    parser: *Parser,

    fn new(messages: *std.ArrayList(Message), parser: *Parser) ParserBenchmark {
        return .{
            .messages = messages,
            .parser = parser,
        };
    }

    pub fn run(self: ParserBenchmark, _: std.mem.Allocator) void {
        const body = [_]u8{97} ** constants.message_max_body_size;
        const encoded_message = ([_]u8{ 23, 215, 237, 54, 195, 69, 158, 226, 0, 0, 0, 0, 0, 0, 0, 0, 148, 217, 184, 248, 40, 131, 99, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);

        self.parser.parse(self.messages, &encoded_message) catch unreachable;
    }
};

pub fn BenchmarkMessageEncode(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    message.encode(backing_buf[0..message.size()]);
}

pub fn BenchmarkMessageCompressGzip(_: std.mem.Allocator) void {
    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.headers.compression = .gzip;
    message.headers.compressed = false;
    message.setBody(body);

    message.compress() catch unreachable;
}

pub fn BenchmarkMessageDecompressGzip(_: std.mem.Allocator) void {
    // this body is "a" ** constants.message_max_body_size but compressed with gzip
    const body = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 237, 192, 129, 12, 0, 0, 0, 195, 48, 214, 249, 75, 156, 227, 73, 91, 0, 0, 0, 0, 0, 0, 0, 192, 187, 1, 213, 102, 111, 13, 0, 32, 0, 0 };
    var message = Message.new();
    message.headers.compression = .gzip;
    message.headers.compressed = true;
    message.setBody(&body);

    message.decompress() catch unreachable;
}

pub fn BenchmarkMessageDecode(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const encoded_message = ([_]u8{ 209, 183, 227, 94, 36, 46, 62, 37, 0, 0, 0, 0, 0, 0, 0, 0, 112, 23, 160, 125, 67, 13, 103, 92, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 } ++ body);
    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

// pub fn BenchmarkManagedQueueEnqueue(allocator: std.mem.Allocator) void {
//     var q = ManagedQueue(usize).init(allocator);
//     defer q.deinit();
//
//     q.enqueue(1) catch unreachable;
// }

const BenchmarkUnmanagedQueueEnqueue = struct {
    const Self = @This();

    messages_list: *std.ArrayList(UnmanagedQueue(*Message).NodeType),
    queue: *UnmanagedQueue(*Message),

    fn new(messages_list: *std.ArrayList(UnmanagedQueue(*Message).NodeType), queue: *UnmanagedQueue(*Message)) Self {
        return .{
            .messages_list = messages_list,
            .queue = queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        for (self.messages_list.items) |*node| {
            self.queue.enqueue(node);
        }
        self.queue.reset();
    }
};

const BenchmarkRingBufferEnqueue = struct {
    const Self = @This();

    messages_list: *std.ArrayList(*Message),
    queue: *RingBuffer(*Message),

    fn new(messages_list: *std.ArrayList(*Message), queue: *RingBuffer(*Message)) Self {
        return .{
            .messages_list = messages_list,
            .queue = queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        for (self.messages_list.items) |message| {
            // for every message in the messages array list we should enqueue it in the ring buffer
            self.queue.enqueue(message) catch unreachable;
        }

        self.queue.reset();
    }
};

const BenchmarkMessageQueueEnqueue = struct {
    const Self = @This();

    messages_list: *std.ArrayList(*Message),
    queue: *MessageQueue,

    fn new(messages_list: *std.ArrayList(*Message), queue: *MessageQueue) Self {
        return .{
            .messages_list = messages_list,
            .queue = queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        for (self.messages_list.items) |message| {
            self.queue.enqueue(message);
        }
        self.queue.reset();
    }
};

const BenchmarkManagedQueueEnqueue = struct {
    const Self = @This();

    messages_list: *std.ArrayList(*Message),
    queue: *ManagedQueue(*Message),

    fn new(messages_list: *std.ArrayList(*Message), queue: *ManagedQueue(*Message)) Self {
        return .{
            .messages_list = messages_list,
            .queue = queue,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        for (self.messages_list.items) |message| {
            self.queue.enqueue(message) catch unreachable;
        }
        while (self.queue.dequeue()) |_| {}
    }
};

test "prints system info" {
    const stderr = std.io.getStdErr().writer();
    try stderr.writeAll("--------------------------------------------------------\n");
    try stderr.print("{}\n", .{try zbench.getSystemInfo()});
    try stderr.writeAll("--------------------------------------------------------\n");
}

test "protocol" {
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var parser_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser_messages_gpa.deinit();
    const parser_messages_allocator = parser_messages_gpa.allocator();

    var parser_messages = std.ArrayList(Message).initCapacity(parser_messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer parser_messages.deinit();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    try bench.add("Message.encode", BenchmarkMessageEncode, .{});
    try bench.add("Message.decode", BenchmarkMessageDecode, .{});
    try bench.add("Message.compress gzip", BenchmarkMessageCompressGzip, .{});
    try bench.add("Message.decompress gzip", BenchmarkMessageDecompressGzip, .{});
    try bench.addParam("Parser.parse", &ParserBenchmark.new(&parser_messages, &parser), .{});

    const stderr = std.io.getStdErr().writer();

    try stderr.writeAll("\n");
    try bench.run(stderr);
}

test "datastructures" {
    const allocator = testing.allocator;

    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var messages_list = std.ArrayList(*Message).initCapacity(allocator, 100) catch unreachable;
    defer messages_list.deinit();

    const body = [_]u8{97} ** constants.message_max_body_size;
    for (0..messages_list.capacity) |i| {
        const message = try allocator.create(Message);
        errdefer allocator.destroy(message);

        message.* = Message.new();
        message.headers.message_type = .request;
        message.setTransactionId(@intCast(i + 1));
        message.setTopicName("test");
        message.setBody(&body);

        assert(message.validate() == null);

        messages_list.appendAssumeCapacity(message);
    }

    defer {
        for (messages_list.items) |message| {
            allocator.destroy(message);
        }
    }

    var unmanaged_queue_nodes = std.ArrayList(UnmanagedQueue(*Message).NodeType).initCapacity(allocator, @intCast(
        messages_list.items.len,
    )) catch unreachable;
    defer unmanaged_queue_nodes.deinit();

    for (messages_list.items) |message| {
        unmanaged_queue_nodes.appendAssumeCapacity(UnmanagedQueue(*Message).NodeType.new(message));
    }

    var ring_buffer = try RingBuffer(*Message).init(allocator, @intCast(messages_list.items.len));
    defer ring_buffer.deinit();

    var managed_queue = ManagedQueue(*Message).init(allocator);
    defer managed_queue.deinit();

    var unmanaged_queue = UnmanagedQueue(*Message).new();
    defer unmanaged_queue.reset();

    var message_queue = MessageQueue.new();
    defer message_queue.reset();

    try bench.addParam("ManagedQueue.enqueue", &BenchmarkManagedQueueEnqueue.new(&messages_list, &managed_queue), .{});
    try bench.addParam("UnmanagedQueue.enqueue", &BenchmarkUnmanagedQueueEnqueue.new(&unmanaged_queue_nodes, &unmanaged_queue), .{});
    try bench.addParam("RingBuffer.enqueue", &BenchmarkRingBufferEnqueue.new(&messages_list, &ring_buffer), .{});
    try bench.addParam("MessageQueue.enqueue", &BenchmarkMessageQueueEnqueue.new(&messages_list, &message_queue), .{});

    const stderr = std.io.getStdErr().writer();

    try stderr.writeAll("\n");
    try bench.run(stderr);
}
