const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const gzip = std.compress.gzip;

const constants = @import("./constants.zig");
const utils = @import("./utils.zig");
const hash = @import("./hash.zig");
const ProtocolError = @import("./errors.zig").ProtocolError;

pub const MessageType = enum(u8) {
    Undefined,
    Request,
    Reply,
    Ping,
    Pong,
};

pub const ErrorCode = enum(u8) {
    Ok,
    Error,
    Unauthorized,
    Timeout,
};

pub const Compression = enum(u8) {
    None,
    Gzip,
};

pub const Message = struct {
    const Self = @This();
    headers: Headers,
    body_buffer: [constants.message_max_body_size]u8,

    // create an uninitialized message container
    pub fn new() Message {
        return Message{
            .headers = Headers{ .message_type = .Undefined },
            .body_buffer = undefined,
        };
    }

    // this needs to be a mut ref to self
    pub fn body(self: *Self) []const u8 {
        assert(self.headers.body_length <= constants.message_max_body_size);

        return self.body_buffer[0..self.headers.body_length];
    }

    pub fn size(self: Self) u32 {
        return @sizeOf(Headers) + self.headers.body_length;
    }

    /// copy a slice of bytes into the body_buffer of the message
    pub fn setBody(self: *Self, v: []const u8) void {
        assert(v.len <= constants.message_max_body_size);

        // copy v into the body_buffer
        @memcpy(self.body_buffer[0..v.len], v);

        // ensure the header.body_length
        self.headers.body_length = @intCast(v.len);
    }

    /// Compresses the body of the message according to the compression algorithim set in message headers.
    /// Does not allow for overcompression which would put the message body in an unknowable state for
    /// the consumer of the message body.
    pub fn compress(self: *Self) !void {
        // ensure that the message is not already compressed
        if (self.headers.compressed) return error.AlreadyCompressed;
        switch (self.headers.compression) {
            .None => {},
            .Gzip => {
                const message_body = self.body();

                // if the message.body is empty, do nothing and just return early this will be faster
                // and can eliminate errors. This should be checked on the decompress call
                if (message_body.len == 0) return;

                // create a reader that can read the message_body
                var reader_fbs = std.io.fixedBufferStream(message_body);
                const reader = reader_fbs.reader();

                // at worse this message is going to be as big as the body already is. This isn't ideal.
                // TODO: remove the requirement to have a separate buffer
                var writer_buf: [constants.message_max_body_size]u8 = undefined;
                var writer_fba = std.heap.FixedBufferAllocator.init(&writer_buf);
                const writer_allocator = writer_fba.allocator();

                var writer_list = std.ArrayList(u8).initCapacity(writer_allocator, writer_buf.len) catch unreachable;
                defer writer_list.deinit();
                const writer = writer_list.writer();

                try gzip.compress(reader, writer, .{});

                // set the message body
                self.setBody(writer_list.items);
                self.headers.compressed = true;
            },
        }
    }

    /// Decompresses the body of the message according to the compression algorithim set in message headers.
    /// Does not allow for overdecompression which would put the message body in an unknowable state for
    /// the consumer of the message body.
    pub fn decompress(self: *Self) !void {
        // ensure that the message is not already compressed
        if (!self.headers.compressed and self.headers.compression != .None) return error.AlreadyDecompressed;
        switch (self.headers.compression) {
            .None => {},
            .Gzip => {
                const message_body = self.body();
                // if the message.body is empty, do nothing and just return early this will be faster
                // and can eliminate errors. This should be checked on the compress call
                if (message_body.len == 0) return;

                // create a reader that can read the message_body
                var reader_fbs = std.io.fixedBufferStream(message_body);
                const reader = reader_fbs.reader();

                // at worse this message is going to be as big as the body already is.
                var writer_buf: [constants.message_max_body_size]u8 = undefined;
                var writer_fba = std.heap.FixedBufferAllocator.init(&writer_buf);
                const writer_allocator = writer_fba.allocator();

                var writer_list = std.ArrayList(u8).initCapacity(writer_allocator, writer_buf.len) catch unreachable;
                defer writer_list.deinit();
                const writer = writer_list.writer();

                try gzip.decompress(reader, writer);

                // set the message body
                self.setBody(writer_list.items);
                self.headers.compressed = false;
            },
        }
    }

    /// encodes the message into bytes that can be sent through a socket
    pub fn encode(self: *Self, buf: []u8) void {
        assert(self.size() <= constants.message_max_size);
        assert(buf.len == self.size());

        // create a buffer that can hold the entirety of the headers payload
        var headers_buf: [@sizeOf(Headers)]u8 = undefined;
        const headers_checksum_payload = self.headers.toChecksumPayload(&headers_buf);

        self.headers.headers_checksum = hash.checksumV3(headers_checksum_payload);
        self.headers.body_checksum = hash.checksumV3(self.body());

        @memcpy(buf[0..@sizeOf(Headers)], std.mem.asBytes(&self.headers));
        @memcpy(buf[@sizeOf(Headers)..self.size()], self.body());
    }

    // take a slice of bytes and try to decode a message out of it
    pub fn decode(self: *Self, data: []const u8) !void {
        // we should not get to this point but just code defensively
        if (data.len < @sizeOf(Headers)) return ProtocolError.NotEnoughData;

        // try to parse the headers out of the buffer
        var headers: Headers = undefined;
        @memcpy(std.mem.asBytes(&headers), data[0..@sizeOf(Headers)]);

        self.headers = headers;

        // create a buffer that can hold the entirety of the headers payload
        var headers_buf: [@sizeOf(Headers)]u8 = undefined;

        // create a payload that is used to calculate the checksum of the headers
        const headers_checksum_payload = self.headers.toChecksumPayload(&headers_buf);

        // compare the header_checksum received from the data against a recomputed checksum based on the values provided
        if (!hash.verifyV3(self.headers.headers_checksum, headers_checksum_payload)) {
            return ProtocolError.InvalidHeaderChecksum;
        }

        // this means that we don't have the full message body and should wait for more data.
        if (data.len < self.size()) return ProtocolError.NotEnoughData;

        // get the body of the message
        const body_bytes = data[@sizeOf(Headers)..self.size()];

        // verify the body checksum to ensure the body is valid
        if (!hash.verifyV3(self.headers.body_checksum, body_bytes)) {
            return ProtocolError.InvalidBodyChecksum;
        }

        self.setBody(body_bytes);
    }
};

pub const Headers = extern struct {
    const Self = @This();
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    origin_id: u64 = 0,
    body_length: u32 = 0,
    protocol_version: u8 = 0,
    message_type: MessageType = .Undefined,
    compression: Compression = .None,
    compressed: bool = false,
    padding: [32]u8 = [_]u8{0} ** 32,

    reserved: [128]u8 = [_]u8{0} ** 128,

    pub fn Type(comptime message_type: MessageType) type {
        return switch (message_type) {
            .Request => Request,
            .Reply => Reply,
            .Ping => Ping,
            .Pong => Pong,
            .Undefined => unreachable,
        };
    }

    pub fn into(self: *Headers, comptime message_type: MessageType) ?*Type(message_type) {
        if (self.message_type != message_type) return null;
        return std.mem.bytesAsValue(Type(message_type), std.mem.asBytes(self));
    }

    pub fn intoConst(self: *const Headers, comptime message_type: MessageType) ?*const Type(message_type) {
        if (self.message_type != message_type) return null;
        return std.mem.bytesAsValue(Type(message_type), std.mem.asBytes(self));
    }

    fn toChecksumPayload(self: Headers, buf: *[@sizeOf(Headers)]u8) []const u8 {
        // the buffer passed into this function should be at least of size [@sizeOf(Header)]u8
        // this could be more efficient in size by removing the 2 checksum bytes sets
        // from this buffer but that seems like more work than it's worth
        assert(buf.len >= @sizeOf(Headers));

        // create a fixed buffer allocator to write the values that should be checksummed
        var fba = std.heap.FixedBufferAllocator.init(buf);
        const fba_allocator = fba.allocator();

        var list = std.ArrayList(u8).initCapacity(fba_allocator, @sizeOf(Headers)) catch unreachable;

        // this excludes header_checksum & body_checksum
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(0)); // headers_checksum
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(0)); // body_checksum
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(self.origin_id));
        list.appendSliceAssumeCapacity(&utils.u32ToBytes(self.body_length));
        list.appendAssumeCapacity(self.protocol_version);
        list.appendAssumeCapacity(@intFromEnum(self.message_type));
        list.appendAssumeCapacity(@intFromEnum(self.compression));
        list.appendAssumeCapacity(@intFromBool(self.compressed));
        list.appendSliceAssumeCapacity(&self.padding);
        list.appendSliceAssumeCapacity(&self.reserved);

        assert(list.items.len == @sizeOf(Headers));
        return list.items;
    }
};

pub const Request = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    origin_id: u64 = 0,
    body_length: u32 = 0,
    protocol_version: u8 = 0,
    message_type: MessageType = .Request,
    compression: Compression = .None,
    compressed: bool = false,
    padding: [32]u8 = [_]u8{0} ** 32,

    transaction_id: u64 = 0,

    reserved: [120]u8 = [_]u8{0} ** 120,
};

pub const Reply = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    origin_id: u64 = 0,
    body_length: u32 = 0,
    protocol_version: u8 = 0,
    message_type: MessageType = .Request,
    compression: Compression = .None,
    compressed: bool = false,
    padding: [32]u8 = [_]u8{0} ** 32,

    transaction_id: u64 = 0,
    error_code: ErrorCode = .Ok,

    reserved: [119]u8 = [_]u8{0} ** 119,
};

pub const Ping = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    origin_id: u64 = 0,
    body_length: u32 = 0,
    protocol_version: u8 = 0,
    message_type: MessageType = .Request,
    compression: Compression = .None,
    compressed: bool = false,
    padding: [32]u8 = [_]u8{0} ** 32,

    reserved: [128]u8 = [_]u8{0} ** 128,
};

pub const Pong = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    origin_id: u64 = 0,
    body_length: u32 = 0,
    protocol_version: u8 = 0,
    message_type: MessageType = .Request,
    compression: Compression = .None,
    compressed: bool = false,
    padding: [32]u8 = [_]u8{0} ** 32,

    reserved: [128]u8 = [_]u8{0} ** 128,
};
