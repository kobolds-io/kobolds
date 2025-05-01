const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

const Message = @import("./message.zig").Message;
const Headers = @import("./message.zig").Headers;
const Request = @import("./message.zig").Request;
const Reply = @import("./message.zig").Reply;
const Ping = @import("./message.zig").Ping;
const Pong = @import("./message.zig").Pong;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;

const ProtocolError = @import("../errors.zig").ProtocolError;

test "encoding" {
    const allocator = std.testing.allocator;

    // this live as long as the scope of this function
    const body = "hello world";
    const topic = "/hello/world";
    var message = Message.new();
    message.headers.message_type = .publish;
    message.headers.origin_id = 332665699182789392398147937282310771713;
    message.setBody(body);
    message.setTopicName(topic);

    const want = [_]u8{ 245, 25, 8, 61, 130, 1, 219, 126, 92, 103, 13, 67, 125, 160, 23, 112, 250, 69, 21, 73, 126, 56, 247, 180, 154, 113, 222, 124, 176, 163, 148, 1, 0, 0, 0, 11, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 };

    const buf = try allocator.alloc(u8, message.size());
    defer allocator.free(buf);

    message.encode(buf);

    // std.debug.print("buf {any}\n", .{buf});

    try std.testing.expect(std.mem.eql(u8, &want, buf));
}

test "decoding" {
    const allocator = std.testing.allocator;

    const body = "a" ** constants.message_max_body_size;
    const topic_name = "/hello/world";

    var original_message = Message.new();
    original_message.headers.message_type = .request;

    original_message.setBody(body);
    original_message.setTopicName(topic_name);

    const encoded_message = try allocator.alloc(u8, original_message.size());
    defer allocator.free(encoded_message);

    // encode the message for transport
    original_message.encode(encoded_message);
    // std.debug.print("encoded message {any}\n", .{encoded_message});

    var decoded_message = Message.new();
    try decoded_message.decode(encoded_message);

    try std.testing.expectEqual(original_message.headers.message_type, decoded_message.headers.message_type);
    try std.testing.expect(std.mem.eql(u8, original_message.body(), decoded_message.body()));
    try std.testing.expect(std.mem.eql(u8, original_message.topicName(), decoded_message.topicName()));
}

test "topic operations" {
    var req_message = Message.new();
    req_message.headers.message_type = .request;

    var topic_name = req_message.topicName();

    try std.testing.expectEqual(0, topic_name.len);

    // set the topic_name
    const new_topic_name = "/hello/world";
    req_message.setTopicName(new_topic_name);

    topic_name = req_message.topicName();

    try std.testing.expect(std.mem.eql(u8, new_topic_name, topic_name));
}

test "constructing and casting Headers" {
    var headers = Headers{ .message_type = .request };

    // ensure that the reserved field is empty
    try std.testing.expect(headers.reserved.len > 0);
    for (headers.reserved) |byte| {
        try std.testing.expectEqual(0, byte);
    }

    const req_headers: *Request = headers.into(.request).?;

    // assert that the request is the correct type w/ correct fields
    try std.testing.expect(@TypeOf(req_headers) == *Request);
    try std.testing.expectEqual(headers.message_type, req_headers.message_type);

    req_headers.transaction_id = 12345;

    const transaction_id_bytes = std.mem.asBytes(&req_headers.transaction_id);

    try std.testing.expect(std.mem.eql(u8, headers.reserved[0..transaction_id_bytes.len], transaction_id_bytes));
}

test "compression: none" {
    const body = "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    // assert that the message is constructed in the way we want.
    try std.testing.expectEqual(body.len, message.body().len);
    try std.testing.expectEqual(message.headers.compression, .none);
    try std.testing.expect(std.mem.eql(u8, body, message.body()));

    // this compression call should do nothing
    try message.compress();

    try std.testing.expectEqual(body.len, message.body().len);
    try std.testing.expect(std.mem.eql(u8, body, message.body()));
    try std.testing.expectEqual(false, message.headers.compressed);
}

test "compression: gzip" {
    const body = "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);
    message.headers.compression = .gzip;

    // assert that the message is constructed in the way we want.
    try std.testing.expectEqual(body.len, message.body().len);
    try std.testing.expectEqual(.gzip, message.headers.compression);
    try std.testing.expectEqual(false, message.headers.compressed);
    try std.testing.expect(std.mem.eql(u8, body, message.body()));

    // this compression call should gzip the body
    try message.compress();

    const want_gzip = [_]u8{ 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 237, 192, 129, 12, 0, 0, 0, 195, 48, 214, 249, 75, 156, 227, 73, 91, 0, 0, 0, 0, 0, 0, 0, 192, 187, 1, 213, 102, 111, 13, 0, 32, 0, 0 };

    // expect that the gzipped value is consistent
    try std.testing.expect(std.mem.eql(u8, &want_gzip, message.body()));
    try std.testing.expectEqual(true, message.headers.compressed);

    // expect that you cannot overcompress the message
    try std.testing.expectError(error.AlreadyCompressed, message.compress());

    // decompress the message
    try message.decompress();

    // ensure that the decompressed contents match the original contents
    try std.testing.expectEqual(body.len, message.body().len);
    try std.testing.expectEqual(false, message.headers.compressed);
    try std.testing.expect(std.mem.eql(u8, body, message.body()));

    // expect that you cannot overdecompress the message
    try std.testing.expectError(error.AlreadyDecompressed, message.decompress());
}

test "headers validation" {
    // Test Request headers
    var request_headers = Request{};
    try std.testing.expect(request_headers.validate() != null);

    request_headers.transaction_id = 1;
    request_headers.topic_name_length = 1; // FIX: this is a hack to pass validation

    try std.testing.expectEqual(null, request_headers.validate());

    // Test Reply Headers
    var reply_headers = Reply{};
    try std.testing.expect(reply_headers.validate() != null);

    reply_headers.transaction_id = 1;
    reply_headers.topic_name_length = 1; // FIX: this is a hack to pass validation
    reply_headers.error_code = .ok;

    try std.testing.expectEqual(null, reply_headers.validate());

    // Test Ping headers
    var ping_headers = Ping{};
    try std.testing.expect(ping_headers.validate() != null);

    ping_headers.transaction_id = 1;

    try std.testing.expectEqual(null, ping_headers.validate());

    // Test Pong Headers
    var pong_headers = Pong{};
    try std.testing.expect(pong_headers.validate() != null);

    pong_headers.transaction_id = 1;
    pong_headers.error_code = .ok;

    try std.testing.expectEqual(null, pong_headers.validate());
}

test "refs and derefs" {
    const allocator = testing.allocator;

    var message_pool = try MessagePool.init(allocator, 100);
    defer message_pool.deinit();

    const message_1 = try Message.create(&message_pool);
    const message_2 = try Message.create(&message_pool);

    try testing.expectEqual(2, message_pool.count());

    try testing.expectEqual(1, message_1.refs());
    try testing.expectEqual(1, message_2.refs());

    message_1.deref();

    try testing.expectEqual(1, message_pool.count());

    message_2.deref();

    try testing.expectEqual(0, message_pool.count());
}

test "rust encoding/zig encoding" {
    const allocator = testing.allocator;
    // this live as long as the scope of this function
    const body = "hello world!";
    var message = Message.new();
    message.headers.message_type = .ping;
    message.headers.origin_id = 0;
    message.setBody(body);

    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    // std.debug.print("zig headers_checksum {any}\n", .{message.headers.headers_checksum});
    // std.debug.print("zig body_checksum {any}\n", .{message.headers.body_checksum});

    const rust_encoding: []const u8 = &[_]u8{ 144, 166, 189, 160, 94, 42, 182, 84, 228, 172, 99, 56, 142, 204, 94, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33 };

    // std.debug.print("zig encoded message {any}\n", .{encoded_message});
    // std.debug.print("rust encoded message {any}\n", .{rust_encoding});

    try testing.expectEqual(rust_encoding.len, encoded_message.len);
    try testing.expect(std.mem.eql(u8, rust_encoding, encoded_message));
}
