const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const constants = @import("./constants.zig");
const hash = @import("./hash.zig");

const ProtocolError = @import("./errors.zig").ProtocolError;
const MessageType = @import("./message_type.zig").MessageType;
const utils = @import("./utils.zig");

pub const Message = struct {
    const Self = @This();

    header: Header,
    body_buffer: [constants.max_message_body_size]u8,

    /// create a new initialized empty message
    pub fn new() Self {
        return Self{
            .header = Header.new(),
            .body_buffer = undefined,
        };
    }

    /// total size of the message
    pub fn size(self: Self) u32 {
        return @sizeOf(Header) + self.header.content_length;
    }

    // copy a slice of bytes into the body_buffer of the message
    pub fn setBody(self: *Self, v: []const u8) void {
        assert(v.len <= constants.max_message_body_size);

        // copy v into the body_buffer
        @memcpy(self.body_buffer[0..v.len], v);

        // ensure the header.content_length
        self.header.content_length = @intCast(v.len);
    }

    /// public getter for the body
    pub fn body(self: *Self) []u8 {
        return self.body_buffer[0..self.header.content_length];
    }

    /// encode a message into a slice of bytes
    pub fn encode(allocator: std.mem.Allocator, message: *Message) ![]const u8 {
        var buf: [constants.max_message_size]u8 = undefined;
        const encoded_message = try Message.encode_(&buf, message);

        const result = try allocator.alloc(u8, encoded_message.len);
        @memcpy(result[0..encoded_message.len], encoded_message);

        return result;
    }

    // helper functions --------------------------------------
    fn encode_(buf: *[constants.max_message_size]u8, message: *Message) ![]const u8 {
        var header_buf: [@sizeOf(Header)]u8 = undefined;
        try message.header.toChecksumPayload(&header_buf);

        message.header.header_checksum = hash.checksumV3(&header_buf);
        message.header.body_checksum = hash.checksumV3(message.body());

        @memcpy(buf[0..@sizeOf(Header)], std.mem.asBytes(&message.header));
        @memcpy(buf[@sizeOf(Header)..message.size()], message.body());

        return buf[0..message.size()];
    }

    /// take a slice of bytes and try to decode a message out of it
    pub fn decode(allocator: std.mem.Allocator, data: []const u8) !*Message {
        var message = Message.new();
        try Message.decode_(&message, data);

        const msg_ptr = try allocator.create(Message);
        msg_ptr.* = message;

        return msg_ptr;
    }

    // take a slice of bytes and try to decode a message out of it
    pub fn decode_(message: *Message, data: []const u8) !void {
        // we should not get to this point but just code defensively
        if (data.len < @sizeOf(Header)) return ProtocolError.NotEnoughData;

        // try to parse the headers out of the buffer
        var header: Header = undefined;
        @memcpy(std.mem.asBytes(&header), data[0..@sizeOf(Header)]);

        message.header = header;

        var buf: [@sizeOf(Header)]u8 = undefined;
        try message.header.toChecksumPayload(&buf);

        // compare the header_checksum received from the data against a recomputed checksum based on the values provided
        if (!hash.verifyV3(message.header.header_checksum, &buf)) {
            return ProtocolError.InvalidHeaderChecksum;
        }

        // this means that we don't have the full message body and should wait for more data.
        if (data.len < message.size()) return ProtocolError.NotEnoughData;

        // get the body of the message
        const body_bytes = data[@sizeOf(Header)..message.size()];

        // verify the body checksum to ensure the body is valid
        if (!hash.verifyV3(message.header.body_checksum, body_bytes)) {
            return ProtocolError.InvalidBodyChecksum;
        }

        message.setBody(body_bytes);
    }
};

pub const Header = extern struct {
    comptime {
        assert(@sizeOf(Header) == 80);
    }

    const Self = @This();

    id: u64 = 0,
    header_checksum: u64 = 0,
    body_checksum: u64 = 0,
    transaction_id: u64 = 0,
    token: u64 = 0,
    content_length: u32 = 0,
    message_type: MessageType = MessageType.Undefined,
    topic_length: u8 = 0,
    topic: [32]u8 = undefined,

    pub fn new() Self {
        return Self{};
    }

    fn toChecksumPayload(self: Self, buf: []u8) !void {
        // the buffer passed into this function should be at least of size [@sizeOf(Header)]u8
        // this could be more efficient in size by removing the 2 checksum bytes sets
        // from this buffer but that seems like more work than it's worth
        assert(buf.len >= @sizeOf(Header));

        // create a fixed buffer allocator to write the values that should be checksummed
        var fba = std.heap.FixedBufferAllocator.init(buf);
        const fba_allocator = fba.allocator();

        var list = try std.ArrayList(u8).initCapacity(fba_allocator, @sizeOf(Header));
        defer list.deinit();

        // this excludes header_checksum & body_checksum
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(self.id));
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(self.id));
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(self.transaction_id));
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(self.token));
        list.appendSliceAssumeCapacity(&utils.u32ToBytes(self.content_length));
        list.appendAssumeCapacity(@intFromEnum(self.message_type));
        list.appendAssumeCapacity(self.topic_length);
        list.appendSliceAssumeCapacity(&self.topic);
    }
};

test "an encoded messages is decoded to the original message" {
    const allocator = std.testing.allocator;

    var original_message = Message.new();

    original_message.header.id = 1;
    const want_body = "123456789";
    original_message.setBody(want_body);

    // encode the message for transport
    const buf = try Message.encode(allocator, &original_message);
    defer allocator.free(buf);

    try testing.expectEqual(@sizeOf(Header) + want_body.len, buf.len);

    // decode the message
    const decoded_message = try Message.decode(allocator, buf);
    defer allocator.destroy(decoded_message);

    const eql = std.mem.eql;
    try testing.expect(eql(u8, original_message.body(), decoded_message.body()));
    try testing.expectEqual(original_message.header.id, decoded_message.header.id);
    try testing.expectEqual(original_message.header.message_type, decoded_message.header.message_type);
    try testing.expectEqual(original_message.header.message_type, decoded_message.header.message_type);

    try testing.expectEqual(original_message.header.header_checksum, decoded_message.header.header_checksum);
    try testing.expectEqual(original_message.header.body_checksum, decoded_message.header.body_checksum);

    try testing.expectEqual(original_message.header.content_length, decoded_message.header.content_length);
    try testing.expectEqual(original_message.header.transaction_id, decoded_message.header.transaction_id);
}

test "encode is fast" {
    // make a big list of encoded_messages
    const ITERATIONS = 3_500_00;
    const body = "a" ** constants.max_message_body_size;

    var durations_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const durations_allocator = durations_arena.allocator();
    defer durations_arena.deinit();

    var durations = try std.ArrayList(u64).initCapacity(durations_allocator, ITERATIONS);
    defer durations.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    var message = Message.new();
    message.setBody(body);

    var encoded_messages_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const serialized_headers_allocator = encoded_messages_arena.allocator();
    defer encoded_messages_arena.deinit();

    var encoded_messages = std.ArrayList([]const u8).init(serialized_headers_allocator);
    defer encoded_messages.deinit();

    for (0..ITERATIONS) |_| {
        const serialize_start = timer.read();

        var buf: [constants.max_message_size]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const encoded_message_allocator = fba.allocator();

        const encoded_message = try Message.encode(encoded_message_allocator, &message);

        const serialize_end = timer.read();
        durations.appendAssumeCapacity(serialize_end - serialize_start);
        try encoded_messages.append(encoded_message);
    }

    const end = timer.read();

    var duration_sum: u128 = 0;
    for (durations.items) |dur| {
        duration_sum += @intCast(dur);
    }

    const average_duration = duration_sum / ITERATIONS;

    std.log.debug("encode average_duration {d}ns", .{average_duration});
    std.log.debug("encode average_duration {d}us", .{average_duration / std.time.ns_per_us});
    std.log.debug("encode average_duration {d}ms", .{average_duration / std.time.ns_per_ms});
    std.log.debug("encode total_time {d}", .{end});
    std.log.debug("encode total_time {d}us", .{end / std.time.ns_per_us});
    std.log.debug("encode total_time {d}ms", .{end / std.time.ns_per_ms});

    try testing.expect(average_duration / std.time.ns_per_ms < 1);
    try testing.expect(end / std.time.ns_per_ms < 250);
    try testing.expectEqual(ITERATIONS, encoded_messages.items.len);
}

test "decode is fast" {
    const original_message_allocator = std.testing.allocator;

    const ITERATIONS = 3_500_00;
    const body = "a" ** constants.max_message_body_size;
    var original_message = Message.new();
    original_message.setBody(body);

    // encode the message for transport
    const encoded_message = try Message.encode(original_message_allocator, &original_message);
    defer original_message_allocator.free(encoded_message);

    var durations_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const durations_allocator = durations_arena.allocator();
    defer durations_arena.deinit();

    var durations = try std.ArrayList(u64).initCapacity(durations_allocator, ITERATIONS);
    defer durations.deinit();

    var decoded_messages_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const serialized_headers_allocator = decoded_messages_arena.allocator();
    defer decoded_messages_arena.deinit();

    var decoded_messages = std.ArrayList(*Message).init(serialized_headers_allocator);
    defer decoded_messages.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    for (0..ITERATIONS) |_| {
        const serialize_start = timer.read();

        var buf: [constants.max_message_size + 100]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const decoded_message_allocator = fba.allocator();

        const decoded_message = try Message.decode(decoded_message_allocator, encoded_message);
        defer decoded_message_allocator.destroy(decoded_message);

        const serialize_end = timer.read();
        durations.appendAssumeCapacity(serialize_end - serialize_start);
        try decoded_messages.append(decoded_message);
    }

    const end = timer.read();

    var duration_sum: u128 = 0;
    for (durations.items) |dur| {
        duration_sum += @intCast(dur);
    }

    const average_duration = duration_sum / ITERATIONS;

    std.log.debug("decode average_duration {d}ns", .{average_duration});
    std.log.debug("decode average_duration {d}us", .{average_duration / std.time.ns_per_us});
    std.log.debug("decode average_duration {d}ms", .{average_duration / std.time.ns_per_ms});
    std.log.debug("decode total_time {d}", .{end});
    std.log.debug("decode total_time {d}us", .{end / std.time.ns_per_us});
    std.log.debug("decode total_time {d}ms", .{end / std.time.ns_per_ms});

    try testing.expect(average_duration < 600);
    try testing.expect(end / std.time.ns_per_ms < 250);
    try testing.expectEqual(ITERATIONS, decoded_messages.items.len);
}

test "Header.serialize is fast" {
    var serialized_headers_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const serialized_headers_allocator = serialized_headers_arena.allocator();
    defer serialized_headers_arena.deinit();

    var serialized_headers = std.ArrayList([]u8).init(serialized_headers_allocator);
    defer serialized_headers.deinit();

    const ITERATIONS = 3_500_000;

    var durations_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const durations_allocator = durations_arena.allocator();
    defer durations_arena.deinit();

    var durations = try std.ArrayList(u64).initCapacity(durations_allocator, ITERATIONS);
    defer durations.deinit();

    var timer = try std.time.Timer.start();
    defer timer.reset();

    for (0..ITERATIONS) |_| {
        const serialize_start = timer.read();
        const header = Header.new();
        var buf: [@sizeOf(Header)]u8 = undefined;
        try header.toChecksumPayload(&buf);

        const serialize_end = timer.read();
        durations.appendAssumeCapacity(serialize_end - serialize_start);
        try serialized_headers.append(&buf);
    }

    const end = timer.read();

    var duration_sum: u128 = 0;
    for (durations.items) |dur| {
        duration_sum += @intCast(dur);
    }

    const average_duration = duration_sum / ITERATIONS;

    // std.log.debug("serialization average_duration {d}ns", .{average_duration});
    // std.log.debug("serialization average_duration {d}us", .{average_duration / std.time.ns_per_us});
    // std.log.debug("serialization average_duration {d}ms", .{average_duration / std.time.ns_per_ms});
    // std.log.debug("serialization total_time {d}", .{end});
    // std.log.debug("serialization total_time {d}us", .{end / std.time.ns_per_us});
    // std.log.debug("serialization total_time {d}ms", .{end / std.time.ns_per_ms});

    try testing.expect(average_duration < 250);
    try testing.expect(end / std.time.ns_per_ms < 1_000);
    try testing.expectEqual(ITERATIONS, serialized_headers.items.len);
}
