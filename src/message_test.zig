const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const constants = @import("./constants.zig");
const utils = @import("./utils.zig");
const hash = @import("./hash.zig");

const Message = @import("./message.zig").Message;
const Headers = @import("./message.zig").Headers;
const Request = @import("./message.zig").Request;
const Reply = @import("./message.zig").Reply;
const Ping = @import("./message.zig").Ping;
const Pong = @import("./message.zig").Pong;

const ProtocolError = @import("./errors.zig").ProtocolError;

test "encoding" {
    const allocator = std.testing.allocator;

    // this live as long as the scope of this function
    const body = "hello world";
    const topic = "/hello/world";
    var message = Message.new();
    message.headers.message_type = .reply;
    message.setBody(body);
    try message.setTopic(topic);

    const want = [_]u8{ 22, 101, 110, 212, 57, 243, 246, 150, 0, 0, 0, 0, 0, 0, 0, 0, 112, 23, 160, 125, 67, 13, 103, 92, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 47, 104, 101, 108, 108, 111, 47, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 };

    const buf = try allocator.alloc(u8, message.size());
    defer allocator.free(buf);

    message.encode(buf);

    // std.debug.print("buf {any}\n", .{buf});

    try std.testing.expect(std.mem.eql(u8, &want, buf));
}

test "decoding" {
    const allocator = std.testing.allocator;

    const body = "a" ** constants.message_max_body_size;
    const topic = "/hello/world";

    var original_message = Message.new();
    original_message.headers.message_type = .request;

    original_message.setBody(body);
    try original_message.setTopic(topic);

    const encoded_message = try allocator.alloc(u8, original_message.size());
    defer allocator.free(encoded_message);

    // encode the message for transport
    original_message.encode(encoded_message);
    // std.debug.print("encoded message {any}\n", .{encoded_message});

    var decoded_message = Message.new();
    try decoded_message.decode(encoded_message);

    try std.testing.expectEqual(original_message.headers.message_type, decoded_message.headers.message_type);
    try std.testing.expect(std.mem.eql(u8, original_message.body(), decoded_message.body()));
    try std.testing.expect(std.mem.eql(u8, try original_message.topic(), try decoded_message.topic()));
}

test "topic operations" {
    const long_invalid_topic = "a" ** (constants.message_max_topic_name_size + 1);

    var req_message = Message.new();
    req_message.headers.message_type = .request;

    var topic = try req_message.topic();

    try std.testing.expectEqual(0, topic.len);

    // set the topic
    const new_topic = "/hello/world";
    try req_message.setTopic(new_topic);

    topic = try req_message.topic();

    try std.testing.expect(std.mem.eql(u8, new_topic, topic));
    try std.testing.expectError(ProtocolError.InvalidTopicLength, req_message.setTopic(long_invalid_topic));

    var ping_message = Message.new();
    ping_message.headers.message_type = .ping;

    try std.testing.expectError(ProtocolError.InvalidMessageOperation, ping_message.setTopic("hello"));
    try std.testing.expectError(ProtocolError.InvalidMessageOperation, ping_message.topic());
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
