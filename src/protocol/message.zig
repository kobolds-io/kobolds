const std = @import("std");
const assert = std.debug.assert;
const gzip = std.compress.gzip;
const atomic = std.atomic;

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");
const ProtocolError = @import("../errors.zig").ProtocolError;

pub const MessageType = enum(u8) {
    undefined,
    request,
    reply,
    ping,
    pong,
    accept,
    advertise,
    unadvertise,
    publish,
    subscribe,
    unsubscribe,
    auth_response,
    auth_challenge,
    auth_result,
};

pub const ErrorCode = enum(u8) {
    undefined,
    ok,
    err,
    unauthorized,
    timeout,
    service_not_found,
    bad_request,
};

pub const Compression = enum(u8) {
    none,
    gzip,
};

pub const ProtocolVersion = enum(u8) {
    unsupported,
    v1,
};

pub const ChallengeMethod = enum(u8) {
    none,
    token,
};

pub const Message = struct {
    const Self = @This();
    headers: Headers,
    body_buffer: [constants.message_max_body_size]u8,

    // how many times this message is referenced
    ref_count: atomic.Value(u32),

    // create an uninitialized Message
    pub fn new() Message {
        return Message{
            .headers = Headers{ .message_type = .undefined },
            .body_buffer = undefined,
            .ref_count = atomic.Value(u32).init(0),
        };
    }

    pub fn new2(message_type: MessageType) Self {
        return Self{
            .headers = Headers{ .message_type = message_type },
            .body_buffer = undefined,
            .ref_count = atomic.Value(u32).init(0),
        };
    }

    pub fn refs(self: *Self) u32 {
        return self.ref_count.load(.seq_cst);
    }

    pub fn ref(self: *Self) void {
        _ = self.ref_count.fetchAdd(1, .seq_cst);
    }

    pub fn deref(self: *Self) void {
        // this is a logical guard as we should never be dereferencing messages more than we
        // have previously referenced them
        assert(self.refs() > 0);

        _ = self.ref_count.fetchSub(1, .seq_cst);
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

    pub fn setTopicName(self: *Self, v: []const u8) void {
        // This is an absolutely tedious way of handling setting fields.
        switch (self.headers.message_type) {
            .request => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Request = self.headers.into(.request).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .reply => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Reply = self.headers.into(.reply).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .advertise => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Advertise = self.headers.into(.advertise).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .unadvertise => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Unadvertise = self.headers.into(.unadvertise).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .publish => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Publish = self.headers.into(.publish).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .subscribe => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Subscribe = self.headers.into(.subscribe).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .unsubscribe => {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                var headers: *Unsubscribe = self.headers.into(.unsubscribe).?;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            else => unreachable,
        }
    }

    pub fn topicName(self: *Self) []const u8 {
        switch (self.headers.message_type) {
            .request => {
                var headers: *Request = self.headers.into(.request).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .reply => {
                var headers: *Reply = self.headers.into(.reply).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .advertise => {
                var headers: *Advertise = self.headers.into(.advertise).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .unadvertise => {
                var headers: *Unadvertise = self.headers.into(.unadvertise).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .publish => {
                var headers: *Publish = self.headers.into(.publish).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .subscribe => {
                var headers: *Subscribe = self.headers.into(.subscribe).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            .unsubscribe => {
                var headers: *Unsubscribe = self.headers.into(.unsubscribe).?;
                return headers.topic_name[0..@intCast(headers.topic_name_length)];
            },
            else => unreachable,
        }
    }

    pub fn setTransactionId(self: *Self, v: u128) void {
        // This is an absolutely tedious way of handling setting fields.
        switch (self.headers.message_type) {
            .request => {
                var headers: *Request = self.headers.into(.request).?;
                headers.transaction_id = v;
            },
            .reply => {
                var headers: *Reply = self.headers.into(.reply).?;
                headers.transaction_id = v;
            },
            .advertise => {
                var headers: *Advertise = self.headers.into(.advertise).?;
                headers.transaction_id = v;
            },
            .unadvertise => {
                var headers: *Unadvertise = self.headers.into(.unadvertise).?;
                headers.transaction_id = v;
            },
            .ping => {
                var headers: *Ping = self.headers.into(.ping).?;
                headers.transaction_id = v;
            },
            .pong => {
                var headers: *Pong = self.headers.into(.pong).?;
                headers.transaction_id = v;
            },
            .subscribe => {
                var headers: *Subscribe = self.headers.into(.subscribe).?;
                headers.transaction_id = v;
            },
            .unsubscribe => {
                var headers: *Unsubscribe = self.headers.into(.unsubscribe).?;
                headers.transaction_id = v;
            },
            .auth_challenge => {
                var headers: *AuthChallenge = self.headers.into(.auth_challenge).?;
                headers.transaction_id = v;
            },
            .auth_response => {
                var headers: *AuthResponse = self.headers.into(.auth_response).?;
                headers.transaction_id = v;
            },
            .auth_result => {
                var headers: *AuthResult = self.headers.into(.auth_result).?;
                headers.transaction_id = v;
            },
            else => unreachable,
        }
    }

    pub fn transactionId(self: *Self) u128 {
        switch (self.headers.message_type) {
            .request => {
                const headers: *const Request = self.headers.intoConst(.request).?;
                return headers.transaction_id;
            },
            .reply => {
                const headers: *const Reply = self.headers.intoConst(.reply).?;
                return headers.transaction_id;
            },
            .advertise => {
                const headers: *const Advertise = self.headers.intoConst(.advertise).?;
                return headers.transaction_id;
            },
            .unadvertise => {
                const headers: *const Unadvertise = self.headers.intoConst(.unadvertise).?;
                return headers.transaction_id;
            },
            .ping => {
                const headers: *const Ping = self.headers.intoConst(.ping).?;
                return headers.transaction_id;
            },
            .pong => {
                const headers: *const Pong = self.headers.intoConst(.pong).?;
                return headers.transaction_id;
            },
            .subscribe => {
                const headers: *const Subscribe = self.headers.intoConst(.subscribe).?;
                return headers.transaction_id;
            },
            .unsubscribe => {
                const headers: *const Unsubscribe = self.headers.intoConst(.unsubscribe).?;
                return headers.transaction_id;
            },
            .auth_response => {
                const headers: *const AuthResponse = self.headers.intoConst(.auth_response).?;
                return headers.transaction_id;
            },
            .auth_challenge => {
                const headers: *const AuthChallenge = self.headers.intoConst(.auth_challenge).?;
                return headers.transaction_id;
            },
            .auth_result => {
                const headers: *const AuthResult = self.headers.intoConst(.auth_result).?;
                return headers.transaction_id;
            },
            else => unreachable,
        }
    }

    pub fn setErrorCode(self: *Self, v: ErrorCode) void {
        // This is an absolutely tedious way of handling setting fields.
        switch (self.headers.message_type) {
            .reply => {
                var headers: *Reply = self.headers.into(.reply).?;
                headers.error_code = v;
            },
            .pong => {
                var headers: *Pong = self.headers.into(.pong).?;
                headers.error_code = v;
            },
            .auth_result => {
                var headers: *AuthResult = self.headers.into(.auth_result).?;
                headers.error_code = v;
            },
            else => unreachable,
        }
    }

    pub fn errorCode(self: *Self) ErrorCode {
        switch (self.headers.message_type) {
            .reply => {
                const headers: *const Reply = self.headers.intoConst(.reply).?;
                return headers.error_code;
            },
            .pong => {
                const headers: *const Pong = self.headers.intoConst(.pong).?;
                return headers.error_code;
            },
            .auth_result => {
                const headers: *const AuthResult = self.headers.intoConst(.auth_result).?;
                return headers.error_code;
            },
            else => unreachable,
        }
    }

    pub fn setChallengeMethod(self: *Self, v: ChallengeMethod) void {
        // This is an absolutely tedious way of handling setting fields.
        switch (self.headers.message_type) {
            .auth_challenge => {
                var headers: *AuthChallenge = self.headers.into(.auth_challenge).?;
                headers.challenge_method = v;
            },
            .auth_response => {
                var headers: *AuthResponse = self.headers.into(.auth_response).?;
                headers.challenge_method = v;
            },
            else => unreachable,
        }
    }

    pub fn challengeMethod(self: *Self) ErrorCode {
        switch (self.headers.message_type) {
            .auth_challenge => {
                const headers: *const AuthChallenge = self.headers.into(.auth_challenge).?;
                return headers.challenge_method;
            },
            .auth_response => {
                const headers: *const AuthResponse = self.headers.into(.auth_response).?;
                return headers.challenge_method;
            },
            else => unreachable,
        }
    }

    /// Compresses the body of the message according to the compression algorithim set in message headers.
    /// Does not allow for overcompression which would put the message body in an unknowable state for
    /// the consumer of the message body.
    pub fn compress(self: *Self) !void {
        // ensure that the message is not already compressed
        if (self.headers.compressed) return error.AlreadyCompressed;
        switch (self.headers.compression) {
            .none => {},
            .gzip => {
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

                var writer_list = std.array_list.Managed(u8).initCapacity(writer_allocator, writer_buf.len) catch unreachable;
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
        if (!self.headers.compressed and self.headers.compression != .none) return error.AlreadyDecompressed;
        switch (self.headers.compression) {
            .none => {},
            .gzip => {
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

                var writer_list = std.array_list.Managed(u8).initCapacity(writer_allocator, writer_buf.len) catch unreachable;
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

        self.headers.headers_checksum = hash.xxHash64Checksum(headers_checksum_payload);
        self.headers.body_checksum = hash.xxHash64Checksum(self.body());

        // Reuse the headers buf
        const headers_bytes = self.headers.asBytes(&headers_buf);

        @memcpy(buf[0..@sizeOf(Headers)], headers_bytes);
        @memcpy(buf[@sizeOf(Headers)..self.size()], self.body());
    }

    // take a slice of bytes and try to decode a message out of it
    pub fn decode(self: *Self, data: []const u8) !void {
        // we should not get to this point but just code defensively
        if (data.len < @sizeOf(Headers)) return ProtocolError.NotEnoughData;

        // try to parse the headers out of the buffer
        self.headers = try Headers.fromBytes(data[0..@sizeOf(Headers)]);

        // create a payload that is used to calculate the checksum of the headers
        // create a buffer that can hold the entirety of the headers payload
        var headers_buf: [@sizeOf(Headers)]u8 = undefined;
        const headers_checksum_payload = self.headers.toChecksumPayload(&headers_buf);

        // compare the header_checksum received from the data against
        // a recomputed checksum based on the values provided
        if (!hash.xxHash64Verify(self.headers.headers_checksum, headers_checksum_payload)) {
            // std.debug.print("expected: {} , got checksum {}", .{ self.headers.headers_checksum, hash.xxHash64Checksum(headers_checksum_payload) });
            return ProtocolError.InvalidHeadersChecksum;
        }

        // let's just exit if we don't support the protocol version
        switch (self.headers.protocol_version) {
            .v1 => {},
            else => @panic("unsupported version"),
        }

        // this means that we don't have the full message body and should wait for more data.
        if (data.len < self.size()) return ProtocolError.NotEnoughData;

        // get the body of the message
        const body_bytes = data[@sizeOf(Headers)..self.size()];

        // verify the body checksum to ensure the body is valid
        if (!hash.xxHash64Verify(self.headers.body_checksum, body_bytes)) {
            return ProtocolError.InvalidBodyChecksum;
        }

        self.setBody(body_bytes);
    }

    // FIX: this is a BS function and there should be a nicer way to call this instead of
    // repeating the same logic found in Headers.Type just figure out what kind of message headers
    // this is
    pub fn validate(self: Self) ?[]const u8 {
        return switch (self.headers.message_type) {
            .request => {
                const headers: *const Request = self.headers.intoConst(.request).?;
                return headers.validate();
            },
            .reply => {
                const headers: *const Reply = self.headers.intoConst(.reply).?;
                return headers.validate();
            },
            .ping => {
                const headers: *const Ping = self.headers.intoConst(.ping).?;
                return headers.validate();
            },
            .pong => {
                const headers: *const Pong = self.headers.intoConst(.pong).?;
                return headers.validate();
            },
            .accept => {
                const headers: *const Accept = self.headers.intoConst(.accept).?;
                return headers.validate();
            },
            .advertise => {
                const headers: *const Advertise = self.headers.intoConst(.advertise).?;
                return headers.validate();
            },
            .unadvertise => {
                const headers: *const Unadvertise = self.headers.intoConst(.unadvertise).?;
                return headers.validate();
            },
            .publish => {
                const headers: *const Publish = self.headers.intoConst(.publish).?;
                return headers.validate();
            },
            .subscribe => {
                const headers: *const Subscribe = self.headers.intoConst(.subscribe).?;
                return headers.validate();
            },
            .unsubscribe => {
                const headers: *const Unsubscribe = self.headers.intoConst(.unsubscribe).?;
                return headers.validate();
            },
            .auth_response => {
                const headers: *const AuthResponse = self.headers.intoConst(.auth_response).?;
                return headers.validate();
            },
            .auth_challenge => {
                const headers: *const AuthChallenge = self.headers.intoConst(.auth_challenge).?;
                return headers.validate();
            },
            .auth_result => {
                const headers: *const AuthResult = self.headers.intoConst(.auth_result).?;
                return headers.validate();
            },
            else => "unsupported message type",
        };
    }
};

pub const Headers = extern struct {
    const Self = @This();
    pub const padding_len: comptime_int = 10;
    pub const reserved_len: comptime_int = 64;

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .undefined,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    reserved: [Headers.reserved_len]u8 = [_]u8{0} ** Headers.reserved_len,

    pub fn Type(comptime message_type: MessageType) type {
        return switch (message_type) {
            .accept => Accept,
            .advertise => Advertise,
            .auth_response => AuthResponse,
            .auth_challenge => AuthChallenge,
            .auth_result => AuthResult,
            .ping => Ping,
            .pong => Pong,
            .publish => Publish,
            .reply => Reply,
            .request => Request,
            .subscribe => Subscribe,
            .unadvertise => Unadvertise,
            .unsubscribe => Unsubscribe,
            .undefined => unreachable,
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

    pub fn asBytes(self: Headers, buf: *[@sizeOf(Headers)]u8) []const u8 {
        return self.serialize(buf, self.headers_checksum, self.body_checksum);
    }

    // we assume the same byte layout as `asBytes`
    pub fn fromBytes(bytes: []const u8) !Headers {
        assert(bytes.len == @sizeOf(Headers));

        var i: usize = 0;

        const headers_checksum = utils.bytesToU64(bytes[i..][0..8]);
        i += 8;

        const body_checksum = utils.bytesToU64(bytes[i..][0..8]);
        i += 8;

        const origin_id = utils.bytesToU128(bytes[i..][0..16]);
        i += 16;

        const connection_id = utils.bytesToU128(bytes[i..][0..16]);
        i += 16;

        const body_length = utils.bytesToU16(bytes[i..][0..2]);
        i += 2;

        const protocol_version: ProtocolVersion = switch (bytes[i]) {
            0 => ProtocolVersion.unsupported,
            1 => ProtocolVersion.v1,
            else => ProtocolVersion.unsupported,
        };
        i += 1;

        const message_type: MessageType = switch (bytes[i]) {
            0 => MessageType.undefined,
            1 => MessageType.request,
            2 => MessageType.reply,
            3 => MessageType.ping,
            4 => MessageType.pong,
            5 => MessageType.accept,
            6 => MessageType.advertise,
            7 => MessageType.unadvertise,
            8 => MessageType.publish,
            9 => MessageType.subscribe,
            10 => MessageType.unsubscribe,
            11 => MessageType.auth_response,
            12 => MessageType.auth_challenge,
            13 => MessageType.auth_result,
            else => MessageType.undefined,
        };
        i += 1;

        const compression: Compression = switch (bytes[i]) {
            0 => Compression.none,
            1 => Compression.gzip,
            else => Compression.none,
        };
        i += 1;

        const compressed = bytes[i] != 0;
        i += 1;

        var padding: [Headers.padding_len]u8 = undefined;
        @memcpy(&padding, bytes[i..][0..Headers.padding_len]);
        i += Headers.padding_len;

        var reserved: [Headers.reserved_len]u8 = undefined;
        @memcpy(&reserved, bytes[i..][0..Headers.reserved_len]);
        i += Headers.reserved_len;

        return Headers{
            .headers_checksum = headers_checksum,
            .body_checksum = body_checksum,
            .origin_id = origin_id,
            .connection_id = connection_id,
            .body_length = body_length,
            .protocol_version = protocol_version,
            .message_type = message_type,
            .compression = compression,
            .compressed = compressed,
            .padding = padding,
            .reserved = reserved,
        };
    }

    pub fn toChecksumPayload(self: Headers, buf: *[@sizeOf(Headers)]u8) []const u8 {
        return self.serialize(buf, 0, 0);
    }

    fn serialize(
        self: Headers,
        buf: *[@sizeOf(Headers)]u8,
        headers_checksum: u64,
        body_checksum: u64,
    ) []const u8 {
        assert(buf.len >= @sizeOf(Headers));

        var fba = std.heap.FixedBufferAllocator.init(buf);
        const fba_allocator = fba.allocator();

        var list = std.array_list.Managed(u8).initCapacity(fba_allocator, @sizeOf(Headers)) catch unreachable;

        list.appendSliceAssumeCapacity(&utils.u64ToBytes(headers_checksum));
        list.appendSliceAssumeCapacity(&utils.u64ToBytes(body_checksum));
        list.appendSliceAssumeCapacity(&utils.u128ToBytes(self.origin_id));
        list.appendSliceAssumeCapacity(&utils.u128ToBytes(self.connection_id));
        list.appendSliceAssumeCapacity(&utils.u16ToBytes(self.body_length));
        list.appendAssumeCapacity(@intFromEnum(self.protocol_version));
        list.appendAssumeCapacity(@intFromEnum(self.message_type));
        list.appendAssumeCapacity(@intFromEnum(self.compression));
        list.appendAssumeCapacity(@intFromBool(self.compressed));
        list.appendSliceAssumeCapacity(&self.padding);
        list.appendSliceAssumeCapacity(&self.reserved);

        assert(list.items.len == @sizeOf(Headers));
        return list.items;
    }

    pub fn validate(self: @This()) ?[]const u8 {
        _ = self;
        unreachable;
    }
};

pub const Request = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .request,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [15]u8 = [_]u8{0} ** 15,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .request);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";
        return null;
    }
};

pub const Reply = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .reply,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,
    error_code: ErrorCode = .ok,

    reserved: [14]u8 = [_]u8{0} ** 14,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .reply);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        if (self.error_code == .undefined) return "invalid error_code";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Ping = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .ping,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,

    reserved: [48]u8 = [_]u8{0} ** 48,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .ping);

        // common headers
        if (self.body_length > 0) return "invalid body_length";
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Pong = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .pong,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    error_code: ErrorCode = .ok,

    reserved: [47]u8 = [_]u8{0} ** 47,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .pong);

        // common headers
        if (self.body_length > 0) return "invalid body_length";
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";
        if (self.error_code == .undefined) return "invalid error_code";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Accept = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .accept,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    reserved: [64]u8 = [_]u8{0} ** 64,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .accept);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure the origin_id is valid
        if (self.origin_id == 0) return "invalid origin_id";

        // ensure the connection_id is valid
        if (self.connection_id == 0) return "invalid connection_id";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";
        return null;
    }
};

pub const Advertise = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .advertise,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [15]u8 = [_]u8{0} ** 15,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .advertise);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure the body of this message is empty
        if (self.body_length > 0) return "invalid body_length";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Unadvertise = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .unadvertise,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [15]u8 = [_]u8{0} ** 15,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .unadvertise);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure the body of this message is empty
        if (self.body_length > 0) return "invalid body_length";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Publish = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .publish,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [31]u8 = [_]u8{0} ** 31,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .publish);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Subscribe = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .subscribe,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [15]u8 = [_]u8{0} ** 15,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .subscribe);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const Unsubscribe = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .unsubscribe,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = [_]u8{0} ** constants.message_max_topic_name_size,
    topic_name_length: u8 = 0,

    reserved: [15]u8 = [_]u8{0} ** 15,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .unsubscribe);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure this topic_name is valid (no empty topic_names allowed)
        if (self.topic_name_length == 0) return "invalid topic_name_length";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const AuthChallenge = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .auth_challenge,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    challenge_method: ChallengeMethod = .none,

    reserved: [47]u8 = [_]u8{0} ** 47,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .auth_challenge);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

/// The `body_buffer` of the `AuthResponse` message contains the auth_response for the challenge.
/// example 1:
///     message.headers.method = .none
///     message.headers.encoding = .cbor
///     ------- therefore
///     body_buffer = "",
/// example 2:
///     message.headers.method = .token
///     message.headers.encoding = .cbor
///     ------- therefore
///     body_buffer = token,
pub const AuthResponse = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .auth_response,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    challenge_method: ChallengeMethod = .none,

    reserved: [47]u8 = [_]u8{0} ** 47,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .auth_response);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};

pub const AuthResult = extern struct {
    comptime {
        assert(@sizeOf(@This()) == @sizeOf(Headers));
    }

    origin_id: u128 = 0,
    connection_id: u128 = 0,
    headers_checksum: u64 = 0,
    body_checksum: u64 = 0,
    body_length: u16 = 0,
    protocol_version: ProtocolVersion = .v1,
    message_type: MessageType = .auth_result,
    compression: Compression = .none,
    compressed: bool = false,
    padding: [Headers.padding_len]u8 = [_]u8{0} ** Headers.padding_len,

    transaction_id: u128 = 0,
    error_code: ErrorCode = .ok,

    reserved: [47]u8 = [_]u8{0} ** 47,

    pub fn validate(self: @This()) ?[]const u8 {
        assert(self.message_type == .auth_result);

        // common headers
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        for (self.padding) |b| if (b != 0) return "invalid padding";

        // ensure this transaction is valid
        if (self.transaction_id == 0) return "invalid transaction_id";

        // ensure reserved is empty
        for (self.reserved) |b| if (b != 0) return "invalid reserved";

        return null;
    }
};
