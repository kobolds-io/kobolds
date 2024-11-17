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

test "encoding" {
    const allocator = std.testing.allocator;

    // this live as long as the scope of this function
    const body = "hello world";
    var message = Message.new();
    message.headers.message_type = .Reply;
    message.setBody(body);

    const want = [_]u8{ 180, 53, 75, 231, 147, 154, 254, 149, 112, 23, 160, 125, 67, 13, 103, 92, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 };

    const buf = try allocator.alloc(u8, message.size());
    defer allocator.free(buf);

    message.encode(buf);

    // std.debug.print("buf {any}\n", .{buf});

    try std.testing.expect(std.mem.eql(u8, &want, buf));
}

test "decoding" {
    const allocator = std.testing.allocator;

    const body = "a" ** constants.message_max_body_size;
    var original_message = Message.new();
    original_message.setBody(body);
    original_message.headers.message_type = .Request;

    const encoded_message = try allocator.alloc(u8, original_message.size());
    defer allocator.free(encoded_message);

    // encode the message for transport
    original_message.encode(encoded_message);

    var decoded_message = Message.new();
    try decoded_message.decode(encoded_message);

    try std.testing.expectEqual(original_message.headers.message_type, decoded_message.headers.message_type);
    try std.testing.expect(std.mem.eql(u8, original_message.body(), decoded_message.body()));
}

test "constructing and casting Headers" {
    var headers = Headers{ .message_type = .Request };

    // ensure that the reserved field is empty
    try std.testing.expect(headers.reserved.len > 0);
    for (headers.reserved) |byte| {
        try std.testing.expectEqual(0, byte);
    }

    const req_headers: *Request = headers.into(.Request).?;

    // assert that the request is the correct type w/ correct fields
    try std.testing.expect(@TypeOf(req_headers) == *Request);
    try std.testing.expectEqual(headers.message_type, req_headers.message_type);

    req_headers.transaction_id = 12345;

    const transaction_id_bytes = std.mem.asBytes(&req_headers.transaction_id);

    try std.testing.expect(std.mem.eql(u8, headers.reserved[0..transaction_id_bytes.len], transaction_id_bytes));
}

test "compression" {
    // NOTE:
    // does compressing the payload actually help? - yes
    // I think I would only be able to compress the body of the payload
    // not the headers because i would not know the size of the headers to be parsed
    // especially of the bytes are read over multiple read operations. the alternative
    // is to compress the entire message and use a length prefixed protocol which defeats
    // the purpose of having the headers struct

    const gzip = std.compress.gzip;

    const body = "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    const allocator = std.testing.allocator;
    const encoded_message = try allocator.alloc(u8, message.size());
    defer allocator.free(encoded_message);

    message.encode(encoded_message);

    var fbs = std.io.fixedBufferStream(message.body());
    var compress_reader = fbs.reader();

    var writer_list = std.ArrayList(u8).init(allocator);
    defer writer_list.deinit();
    const compress_writer = writer_list.writer();

    try gzip.compress(&compress_reader, compress_writer, .{});

    // std.debug.print("encoded_message {any}\n", .{encoded_message});
    // std.debug.print("encoded_message.len {any}\n", .{encoded_message.len});
    // std.debug.print("compressed_message {any}\n", .{writer_list.items});
    // std.debug.print("compressed_message.len {any}\n", .{writer_list.items.len});
}
