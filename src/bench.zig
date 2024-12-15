const std = @import("std");
const zbench = @import("zbench");
const assert = std.debug.assert;

const Parser = @import("./protocol/parser.zig").Parser;
const Parser2 = @import("./protocol/parser.zig").Parser2;
const Message = @import("./protocol/message.zig").Message;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;
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
        const encoded_message = ([_]u8{ 37, 58, 161, 118, 166, 59, 171, 63, 0, 0, 0, 0, 0, 0, 0, 0, 148, 217, 184, 248, 40, 131, 99, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);

        self.parser.parse(self.messages, &encoded_message) catch unreachable;
    }
};

const Parser2Benchmark = struct {
    messages: *std.ArrayList(Message),
    parser: *Parser2,

    fn new(messages: *std.ArrayList(Message), parser: *Parser2) Parser2Benchmark {
        return .{
            .messages = messages,
            .parser = parser,
        };
    }

    pub fn run(self: Parser2Benchmark, _: std.mem.Allocator) void {
        const body = [_]u8{97} ** constants.message_max_body_size;
        const encoded_message = ([_]u8{ 37, 58, 161, 118, 166, 59, 171, 63, 0, 0, 0, 0, 0, 0, 0, 0, 148, 217, 184, 248, 40, 131, 99, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);

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
    message.headers.compression = .Gzip;
    message.headers.compressed = false;
    message.setBody(body);

    message.compress() catch unreachable;
}

pub fn BenchmarkMessageDecompressGzip(_: std.mem.Allocator) void {
    // this body is "a" ** constants.message_max_body_size but compressed with gzip
    const body = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 237, 192, 129, 12, 0, 0, 0, 195, 48, 214, 249, 75, 156, 227, 73, 91, 0, 0, 0, 0, 0, 0, 0, 192, 187, 1, 213, 102, 111, 13, 0, 32, 0, 0 };
    var message = Message.new();
    message.headers.compression = .Gzip;
    message.headers.compressed = true;
    message.setBody(&body);

    message.decompress() catch unreachable;
}

pub fn BenchmarkMessageDecode(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const encoded_message = ([_]u8{ 37, 58, 161, 118, 166, 59, 171, 63, 0, 0, 0, 0, 0, 0, 0, 0, 148, 217, 184, 248, 40, 131, 99, 98, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);
    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

const MessageQueueEnqueueBenchmark = struct {
    const Self = @This();
    messages: *std.ArrayList(Message),

    fn new(messages: *std.ArrayList(Message)) Self {
        return .{
            .messages = messages,
        };
    }

    pub fn run(self: Self, _: std.mem.Allocator) void {
        var queue = MessageQueue.new(constants.queue_size_max);
        assert(queue.isEmpty());
        for (self.messages.items) |*message| {
            // quickly reset the message
            if (message.next != null) message.next = null;
            queue.enqueue(message) catch unreachable;
        }

        assert(queue.count == constants.queue_size_max);
    }
};

const MessageQueueDequeueBenchmark = struct {
    messages: *std.ArrayList(Message),
    queue: *MessageQueue,

    fn new(messages: *std.ArrayList(Message), queue: *MessageQueue) MessageQueueDequeueBenchmark {
        return .{
            .messages = messages,
            .queue = queue,
        };
    }

    pub fn run(self: MessageQueueDequeueBenchmark, _: std.mem.Allocator) void {
        assert(self.queue.count == constants.queue_size_max);

        for (self.messages.items) |*message| {
            const next = message.next;
            var m = self.queue.dequeue().?;

            m.next = next;
        }

        assert(self.queue.isEmpty());

        // this is a hacky resetting of the queue
        self.queue.head = &self.messages.items[0];
        self.queue.tail = &self.messages.items[self.messages.items.len - 1];
        self.queue.count = @intCast(self.messages.items.len);
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

    var parser2_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser2_messages_gpa.deinit();
    const parser2_messages_allocator = parser2_messages_gpa.allocator();

    var parser2_messages = std.ArrayList(Message).initCapacity(parser2_messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer parser2_messages.deinit();

    var parser2 = Parser2.new();

    try bench.add("Message.encode", BenchmarkMessageEncode, .{});
    try bench.add("Message.decode", BenchmarkMessageDecode, .{});
    try bench.add("Message.compress gzip", BenchmarkMessageCompressGzip, .{});
    try bench.add("Message.decompress gzip", BenchmarkMessageDecompressGzip, .{});
    try bench.addParam("Parser.parse", &ParserBenchmark.new(&parser_messages, &parser), .{});
    try bench.addParam("Parser2.parse", &Parser2Benchmark.new(&parser2_messages, &parser2), .{});

    const stderr = std.io.getStdErr().writer();

    try stderr.writeAll("\n");
    try bench.run(stderr);
}

test "data_structures" {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{});
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var queue_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = queue_messages_gpa.deinit();
    const queue_messages_allocator = queue_messages_gpa.allocator();

    var queue_messages = std.ArrayList(Message).initCapacity(queue_messages_allocator, constants.queue_size_max) catch unreachable;
    defer queue_messages.deinit();

    for (0..constants.queue_size_max) |_| {
        queue_messages.appendAssumeCapacity(Message.new());
    }

    var dequeue_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = dequeue_messages_gpa.deinit();
    const dequeue_messages_allocator = dequeue_messages_gpa.allocator();

    var dequeue_messages = std.ArrayList(Message).initCapacity(dequeue_messages_allocator, constants.queue_size_max) catch unreachable;
    defer dequeue_messages.deinit();

    for (0..constants.queue_size_max) |_| {
        dequeue_messages.appendAssumeCapacity(Message.new());
    }

    var dequeue_queue = MessageQueue.new(constants.queue_size_max);

    // do the initial setup for the Dequeue benchmark test
    for (dequeue_messages.items) |*message| {
        dequeue_queue.enqueue(message) catch unreachable;
    }

    try bench.addParam("MessageQueue.enqueue", &MessageQueueEnqueueBenchmark.new(&queue_messages), .{});
    try bench.addParam("MessageQueue.dequeue", &MessageQueueDequeueBenchmark.new(&dequeue_messages, &dequeue_queue), .{});

    const stderr = std.io.getStdErr().writer();

    try stderr.writeAll("\n");

    try bench.run(stderr);
}
