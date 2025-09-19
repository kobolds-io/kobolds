const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.Message);
const atomic = std.atomic;

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");
const hash = @import("../hash.zig");

pub const ProtocolVersion = enum(u4) {
    unsupported,
    v1,
};

pub const DeserializeResult = struct {
    message: Message,
    bytes_consumed: usize,
};

pub const ChallengeMethod = enum(u8) {
    none,
    token,
};

pub const ChallengeAlgorithm = enum(u8) {
    unsupported,
    hmac256,
};

pub const PeerType = enum(u8) {
    client,
    node,
};

pub const MessageType = enum(u8) {
    undefined,
    publish,
    subscribe,
    auth_challenge,
    session_init,
    session_join,
    auth_failure,
    auth_success,
};

pub const ErrorCode = enum(u8) {
    unsupported,
    unauthenticated,
    unauthorized,
};

pub const Message = struct {
    const Self = @This();

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .undefined = {} },
    body_buffer: [constants.message_max_body_size]u8 = undefined,
    checksum: u64 = 0,

    // how many times this message is referenced
    ref_count: atomic.Value(u32),

    pub fn new(id: u64, message_type: MessageType) Self {
        return switch (message_type) {
            .undefined => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .undefined = {} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .publish => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .publish = PublishHeaders{
                    .message_id = id,
                } },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .subscribe => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .subscribe = SubscribeHeaders{
                    .message_id = id,
                } },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .auth_challenge => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .auth_challenge = AuthChallengeHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .session_init => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .session_init = SessionInitHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .session_join => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .session_join = SessionJoinHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .auth_failure => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .auth_failure = AuthFailureHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
            .auth_success => Self{
                .fixed_headers = .{ .message_type = message_type },
                .extension_headers = .{ .auth_success = AuthSuccessHeaders{} },
                .body_buffer = undefined,
                .checksum = 0,
                .ref_count = atomic.Value(u32).init(0),
            },
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

    pub fn body(self: *Self) []const u8 {
        return self.body_buffer[0..self.fixed_headers.body_length];
    }

    /// copy a slice of bytes into the body_buffer of the message
    pub fn setBody(self: *Self, v: []const u8) void {
        assert(v.len <= constants.message_max_body_size);

        // copy v into the body_buffer
        @memcpy(self.body_buffer[0..v.len], v);

        // ensure the header.body_length
        self.fixed_headers.body_length = @intCast(v.len);
    }

    pub fn packedSize(self: Self) usize {
        var sum: usize = 0;
        sum += FixedHeaders.packedSize();
        sum += self.extension_headers.packedSize();
        sum += self.fixed_headers.body_length;
        sum += @sizeOf(u64);

        return sum;
    }

    pub fn setTopicName(self: *Self, v: []const u8) void {
        switch (self.extension_headers) {
            .publish => |*headers| {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            .subscribe => |*headers| {
                if (v.len > constants.message_max_topic_name_size) unreachable;

                @memcpy(headers.topic_name[0..v.len], v);
                headers.topic_name_length = @intCast(v.len);
            },
            else => unreachable,
        }
    }

    pub fn topicName(self: *Self) []const u8 {
        switch (self.extension_headers) {
            .publish => |headers| return headers.topic_name[0..headers.topic_name_length],
            .subscribe => |headers| return headers.topic_name[0..headers.topic_name_length],
            else => unreachable,
        }
    }

    pub fn setMessageId(self: *Self, v: u64) void {
        switch (self.extension_headers) {
            .publish => |*headers| headers.message_id = v,
            .subscribe => |*headers| headers.message_id = v,
            else => unreachable,
        }
    }

    pub fn messageId(self: *Self) []const u8 {
        switch (self.extension_headers) {
            .publish => |headers| return headers.message_id,
            .subscribe => |headers| return headers.message_id,
            else => unreachable,
        }
    }

    /// Calculate the size of the unserialized message.
    pub fn size(self: Self) usize {
        const extension_size: usize = switch (self.fixed_headers.message_type) {
            .undefined => 0,
            .publish => @sizeOf(PublishHeaders),
            .subscribe => @sizeOf(SubscribeHeaders),
            .auth_challenge => @sizeOf(AuthChallengeHeaders),
            .session_init => @sizeOf(SessionInitHeaders),
            .session_join => @sizeOf(SessionJoinHeaders),
            .auth_failure => @sizeOf(AuthFailureHeaders),
            .auth_success => @sizeOf(AuthSuccessHeaders),
        };

        return @sizeOf(FixedHeaders) + extension_size + self.fixed_headers.body_length + @sizeOf(u64);
    }

    pub fn serialize(self: *Self, buf: []u8) usize {
        // Ensure the buffer is large enough
        assert(buf.len >= self.packedSize());

        var i: usize = 0;

        // Fixed headers
        i += self.fixed_headers.toBytes(buf[i..]);

        // Extension headers
        i += self.extension_headers.toBytes(buf[i..]);

        const body_len = self.fixed_headers.body_length;
        @memcpy(buf[i .. i + body_len], self.body_buffer[0..body_len]);
        i += body_len;

        // Compute checksum directly on the written slice
        const checksum = hash.xxHash64Checksum(buf[0..i]);

        // Write checksum in-place (avoids creating buf[i..][0..size])
        std.mem.writeInt(u64, buf.ptr[i..][0..@sizeOf(u64)], checksum, .big);
        i += @sizeOf(u64);

        // if this didn't work something went wrong
        if (self.packedSize() != i) {
            log.err("self {any}", .{self});
            log.err("i {}", .{i});
            unreachable;
        }

        return i;
    }

    pub fn deserialize(data: []const u8) !DeserializeResult {
        // ensure that the buffer is at least the minimum size that a message could possibly be.
        if (data.len < FixedHeaders.packedSize()) return error.Truncated;

        var read_offset: usize = 0;

        // get the fixed headers from the bytes
        const fixed_headers = try FixedHeaders.fromBytes(data[0..FixedHeaders.packedSize()]);
        read_offset += FixedHeaders.packedSize();

        const extension_headers = try ExtensionHeaders.fromBytes(
            fixed_headers.message_type,
            data[FixedHeaders.packedSize()..],
        );
        const extension_headers_size = extension_headers.packedSize();
        read_offset += extension_headers_size;

        const total_message_size =
            FixedHeaders.packedSize() +
            extension_headers_size +
            fixed_headers.body_length +
            @sizeOf(u64);

        if (data.len < total_message_size) return error.Truncated;

        if (fixed_headers.body_length > constants.message_max_body_size) return error.InvalidMessage;
        if (data[read_offset..].len < fixed_headers.body_length) return error.Truncated;

        var body_buffer: [constants.message_max_body_size]u8 = undefined;
        @memcpy(body_buffer[0..fixed_headers.body_length], data[read_offset .. read_offset + fixed_headers.body_length]);
        read_offset += fixed_headers.body_length;

        if (data[read_offset..].len < @sizeOf(u64)) return error.Truncated;
        const checksum = std.mem.readInt(u64, data[read_offset .. read_offset + @sizeOf(u64)][0..@sizeOf(u64)], .big);

        if (!hash.xxHash64Verify(checksum, data[0..read_offset])) return error.InvalidChecksum;
        read_offset += @sizeOf(u64);

        assert(read_offset == total_message_size);

        return DeserializeResult{
            .message = Message{
                .fixed_headers = fixed_headers,
                .extension_headers = extension_headers,
                .body_buffer = body_buffer,
                .checksum = checksum,
                .ref_count = atomic.Value(u32).init(0),
            },
            .bytes_consumed = read_offset,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.fixed_headers.validate()) |e| return e;
        switch (self.extension_headers) {
            .publish => |headers| if (headers.validate()) |e| return e,
            .subscribe => |headers| if (headers.validate()) |e| return e,
            .auth_challenge => |headers| if (headers.validate()) |e| return e,
            .session_init => |headers| if (headers.validate()) |e| return e,
            .session_join => |headers| if (headers.validate()) |e| return e,
            .auth_failure => |headers| if (headers.validate()) |e| return e,
            .auth_success => |headers| if (headers.validate()) |e| return e,
            else => return "invalid headers",
        }

        return null;
    }
};

pub const FixedHeaders = packed struct {
    const Self = @This();

    comptime {
        assert(6 == Self.packedSize());
    }

    body_length: u16 = 0,
    message_type: MessageType = .undefined,
    protocol_version: ProtocolVersion = .v1,
    flags: u4 = 0,
    padding: u16 = 0,

    pub fn packedSize() usize {
        return @sizeOf(u16) + @sizeOf(MessageType) + 1 + @sizeOf(u16);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= @sizeOf(Self));

        var i: usize = 0;

        std.mem.writeInt(u16, buf[i..][0..2], self.body_length, .big);
        i += @sizeOf(u16);

        buf[i] = @intFromEnum(self.message_type);
        i += 1;

        // protocol_version (u4) + flags (u4) packed into one byte
        buf[i] = (@as(u8, @intFromEnum(self.protocol_version)) << 4) | @as(u8, self.flags);
        i += 1;

        std.mem.writeInt(u16, buf[i..][0..2], self.padding, .big);
        i += @sizeOf(u16);

        return i;
    }

    /// Writes the packed struct into `buf` in big-endian order.
    /// Returns the slice of written bytes.
    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.packedSize()) return error.Truncated;

        var i: usize = 0;

        const body_length = (@as(u16, data[i]) << 8) | @as(u16, data[i + 1]);
        i += 2;

        const message_type: MessageType = switch (data[i]) {
            0 => .undefined,
            1 => .publish,
            2 => .subscribe,
            3 => .auth_challenge,
            4 => .session_init,
            5 => .session_join,
            6 => .auth_failure,
            7 => .auth_success,
            else => return error.InvalidMessageType,
        };
        i += 1;

        const protocol_version_flags = data[i];
        const protocol_version_int: u4 = @intCast(protocol_version_flags >> 4);
        const protocol_version: ProtocolVersion = switch (protocol_version_int) {
            1 => .v1,
            else => .unsupported,
        };
        const flags: u4 = @intCast(protocol_version_flags & 0xF);
        i += 1;

        const padding = (@as(u16, data[i]) << 8) | @as(u16, data[i + 1]);
        i += 2;

        return FixedHeaders{
            .body_length = body_length,
            .message_type = message_type,
            .protocol_version = protocol_version,
            .flags = flags,
            .padding = padding,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.message_type == .undefined) return "invalid message_type";
        if (self.protocol_version == .unsupported) return "invalid protocol_version";
        if (self.padding != 0) return "invalid padding";

        return null;
    }
};

pub const ExtensionHeaders = union(MessageType) {
    const Self = @This();
    undefined: void,
    publish: PublishHeaders,
    subscribe: SubscribeHeaders,
    auth_challenge: AuthChallengeHeaders,
    session_init: SessionInitHeaders,
    session_join: SessionJoinHeaders,
    auth_failure: AuthFailureHeaders,
    auth_success: AuthSuccessHeaders,

    pub fn packedSize(self: *const Self) usize {
        return switch (self.*) {
            .undefined => 0,
            inline else => |headers| headers.packedSize(),
        };
    }

    pub fn toBytes(self: *Self, buf: []u8) usize {
        return switch (self.*) {
            .undefined => 0,
            inline else => |headers| blk: {
                assert(buf.len >= @sizeOf(@TypeOf(headers)));

                const bytes = headers.toBytes(buf);
                break :blk bytes;
            },
        };
    }

    pub fn fromBytes(message_type: MessageType, data: []const u8) !Self {
        return switch (message_type) {
            .undefined => ExtensionHeaders{ .undefined = {} },
            .publish => blk: {
                const headers = try PublishHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .publish = headers };
            },
            .subscribe => blk: {
                const headers = try SubscribeHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .subscribe = headers };
            },
            .auth_challenge => blk: {
                const headers = try AuthChallengeHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .auth_challenge = headers };
            },
            .session_init => blk: {
                const headers = try SessionInitHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .session_init = headers };
            },
            .session_join => blk: {
                const headers = try SessionJoinHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .session_join = headers };
            },
            .auth_failure => blk: {
                const headers = try AuthFailureHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .auth_failure = headers };
            },
            .auth_success => blk: {
                const headers = try AuthSuccessHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .auth_success = headers };
            },
        };
    }
};

pub const PublishHeaders = struct {
    const Self = @This();

    message_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,

    pub fn packedSize(self: Self) usize {
        return Self.minimumSize() + self.topic_name_length;
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.message_id, .big);
        i += @sizeOf(u64);

        buf[i] = self.topic_name_length;
        i += 1;

        @memcpy(buf[i .. i + self.topic_name_length], self.topic_name[0..self.topic_name_length]);
        i += @intCast(self.topic_name_length);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const message_id = std.mem.readInt(u64, data[0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const topic_name_length = data[i];
        i += 1;

        if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
        if (i + topic_name_length > data.len) return error.Truncated;

        var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
        @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);

        return Self{
            .message_id = message_id,
            .topic_name_length = topic_name_length,
            .topic_name = topic_name,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.message_id == 0) return "invalid message_id";
        if (self.topic_name_length == 0) return "invalid topic_name_length";
        if (self.topic_name_length > constants.message_max_topic_name_size) return "invalid topic_name_length";

        return null;
    }
};

pub const SubscribeHeaders = struct {
    const Self = @This();

    message_id: u64 = 0,
    transaction_id: u64 = 0,
    topic_name_length: u8 = 0,
    topic_name: [constants.message_max_topic_name_size]u8 = undefined,

    pub fn packedSize(self: Self) usize {
        return Self.minimumSize() + self.topic_name_length;
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + @sizeOf(u64) + 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.message_id, .big);
        i += @sizeOf(u64);

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.transaction_id, .big);
        i += @sizeOf(u64);

        buf[i] = self.topic_name_length;
        i += 1;

        @memcpy(buf[i .. i + self.topic_name_length], self.topic_name[0..self.topic_name_length]);
        i += @intCast(self.topic_name_length);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const message_id = std.mem.readInt(u64, data[i..@sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const transaction_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const topic_name_length = data[i];
        i += 1;

        if (topic_name_length > constants.message_max_topic_name_size) return error.InvalidTopicName;
        if (i + topic_name_length > data.len) return error.Truncated;

        var topic_name: [constants.message_max_topic_name_size]u8 = undefined;
        @memcpy(topic_name[0..topic_name_length], data[i .. i + topic_name_length]);

        return Self{
            .message_id = message_id,
            .transaction_id = transaction_id,
            .topic_name_length = topic_name_length,
            .topic_name = topic_name,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.message_id == 0) return "invalid message_id";
        if (self.transaction_id == 0) return "invalid transaction_id";
        if (self.topic_name_length == 0) return "invalid topic_name_length";
        if (self.topic_name_length > constants.message_max_topic_name_size) return "invalid topic_name_length";

        return null;
    }
};

pub const AuthChallengeHeaders = struct {
    const Self = @This();

    // challenge method and algorithm can be made smaller
    challenge_method: ChallengeMethod = .none,
    algorithm: ChallengeAlgorithm = .hmac256,
    connection_id: u64 = 0,
    nonce: u128 = 0,

    pub fn packedSize(self: Self) usize {
        _ = self;
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(ChallengeMethod) + @sizeOf(ChallengeAlgorithm) + @sizeOf(u64) + @sizeOf(u128);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());

        var i: usize = 0;
        buf[i] = @intFromEnum(self.challenge_method);
        i += 1;

        buf[i] = @intFromEnum(self.algorithm);
        i += 1;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.connection_id, .big);
        i += @sizeOf(u64);

        std.mem.writeInt(u128, buf[i..][0..@sizeOf(u128)], self.nonce, .big);
        i += @sizeOf(u128);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const challenge_method: ChallengeMethod = switch (data[i]) {
            0 => .none,
            1 => .token,
            else => unreachable, // should probably fail more gracefully
        };
        i += 1;

        const algorithm: ChallengeAlgorithm = switch (data[i]) {
            0 => .unsupported,
            1 => .hmac256,
            else => unreachable, // should probably fail more gracefully
        };
        i += 1;

        const connection_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const nonce = std.mem.readInt(u128, data[i .. i + @sizeOf(u128)][0..@sizeOf(u128)], .big);
        i += @sizeOf(u128);

        return Self{
            .challenge_method = challenge_method,
            .algorithm = algorithm,
            .connection_id = connection_id,
            .nonce = nonce,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.connection_id == 0) return "invalid connection_id";
        if (self.nonce == 0) return "invalid nonce";

        return null;
    }
};

pub const SessionInitHeaders = struct {
    const Self = @This();

    peer_id: u64 = 0,
    peer_type: PeerType = .client,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(PeerType) + @sizeOf(u64);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());
        var i: usize = 0;

        buf[i] = @intFromEnum(self.peer_type);
        i += 1;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.peer_id, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const peer_type: PeerType = switch (data[i]) {
            0 => .client,
            1 => .node,
            else => unreachable,
        };
        i += 1;

        const peer_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        return Self{
            .peer_id = peer_id,
            .peer_type = peer_type,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.peer_id == 0) return "invalid peer_id";

        return null;
    }
};

pub const SessionJoinHeaders = struct {
    const Self = @This();

    peer_id: u64 = 0,
    session_id: u64 = 0,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + @sizeOf(u64);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.peer_id, .big);
        i += @sizeOf(u64);

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.session_id, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const peer_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const session_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        return Self{
            .peer_id = peer_id,
            .session_id = session_id,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.peer_id == 0) return "invalid peer_id";
        if (self.session_id == 0) return "invalid session_id";

        return null;
    }
};

pub const AuthFailureHeaders = struct {
    const Self = @This();

    error_code: ErrorCode = .unauthenticated,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(ErrorCode);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());
        var i: usize = 0;

        buf[i] = @intFromEnum(self.error_code);
        i += 1;

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const error_code: ErrorCode = switch (data[i]) {
            1 => .unauthenticated,
            2 => .unauthorized,
            else => .unsupported,
        };
        i += 1;

        return Self{
            .error_code = error_code,
        };
    }

    pub fn validate(_: Self) ?[]const u8 {
        return null;
    }
};

pub const AuthSuccessHeaders = struct {
    const Self = @This();

    peer_id: u64 = 0,
    session_id: u64 = 0,

    pub fn packedSize(_: Self) usize {
        return Self.minimumSize();
    }

    fn minimumSize() usize {
        return @sizeOf(u64) + @sizeOf(u64);
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= self.packedSize());
        var i: usize = 0;

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.peer_id, .big);
        i += @sizeOf(u64);

        std.mem.writeInt(u64, buf[i..][0..@sizeOf(u64)], self.session_id, .big);
        i += @sizeOf(u64);

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        var i: usize = 0;

        if (data.len < Self.minimumSize()) return error.Truncated;

        const peer_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        const session_id = std.mem.readInt(u64, data[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], .big);
        i += @sizeOf(u64);

        return Self{
            .session_id = session_id,
            .peer_id = peer_id,
        };
    }

    pub fn validate(self: Self) ?[]const u8 {
        if (self.peer_id == 0) return "invalid peer_id";
        if (self.session_id == 0) return "invalid session_id";

        return null;
    }
};

test "size of structs" {
    try testing.expectEqual(8, @sizeOf(FixedHeaders));
    try testing.expectEqual(6, FixedHeaders.packedSize());

    const undefined_message = Message.new(0, .undefined);
    try testing.expectEqual(16, undefined_message.size());
    try testing.expectEqual(14, undefined_message.packedSize());

    const publish_message = Message.new(0, .publish);
    try testing.expectEqual(64, publish_message.size());
    try testing.expectEqual(23, publish_message.packedSize());

    const subscribe_message = Message.new(0, .subscribe);
    try testing.expectEqual(72, subscribe_message.size());
    try testing.expectEqual(31, subscribe_message.packedSize());

    const session_init_message = Message.new(0, .session_init);
    try testing.expectEqual(32, session_init_message.size());
    try testing.expectEqual(23, session_init_message.packedSize());

    const session_join_message = Message.new(0, .session_join);
    try testing.expectEqual(32, session_join_message.size());
    try testing.expectEqual(30, session_join_message.packedSize());

    const auth_failure_message = Message.new(0, .auth_failure);
    try testing.expectEqual(17, auth_failure_message.size());
    try testing.expectEqual(15, auth_failure_message.packedSize());

    const auth_success_message = Message.new(0, .auth_success);
    try testing.expectEqual(32, auth_success_message.size());
    try testing.expectEqual(30, auth_success_message.packedSize());
}

test "message can comprise of variable size extensions" {
    const publish_message = Message.new(0, .publish);
    try testing.expectEqual(publish_message.fixed_headers.message_type, .publish);
}

test "message serialization" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
        .subscribe,
        .session_init,
        .session_join,
        .auth_failure,
        .auth_success,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(111, message_type);

        const bytes = message.serialize(&buf);

        try testing.expectEqual(bytes, message.packedSize());
    }
}

test "message deserialization" {
    const message_types = [_]MessageType{
        .undefined,
        .publish,
        .subscribe,
        .session_init,
        .session_join,
        .auth_failure,
        .auth_success,
    };

    var buf: [@sizeOf(Message)]u8 = undefined;

    for (message_types) |message_type| {
        var message = Message.new(300, message_type);

        // serialize the message
        const bytes = message.serialize(&buf);

        // deserialize the message
        const deserialized_result = try Message.deserialize(buf[0..bytes]);

        try testing.expectEqual(bytes, deserialized_result.bytes_consumed);

        var deserialized_message = deserialized_result.message;

        try testing.expectEqual(message.size(), deserialized_message.size());
        try testing.expectEqual(message.packedSize(), deserialized_message.packedSize());
        try testing.expect(std.mem.eql(u8, message.body(), deserialized_message.body()));

        switch (message_type) {
            .undefined => {
                try testing.expectEqual(message.size(), deserialized_message.size());
            },
            .publish => {
                try testing.expectEqual(
                    message.extension_headers.publish.message_id,
                    deserialized_message.extension_headers.publish.message_id,
                );
                try testing.expectEqual(
                    message.extension_headers.publish.topic_name_length,
                    deserialized_message.extension_headers.publish.topic_name_length,
                );
                try testing.expect(std.mem.eql(u8, message.topicName(), deserialized_message.topicName()));
            },
            .subscribe => {
                try testing.expectEqual(
                    message.extension_headers.subscribe.message_id,
                    deserialized_message.extension_headers.subscribe.message_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.transaction_id,
                    deserialized_message.extension_headers.subscribe.transaction_id,
                );
                try testing.expectEqual(
                    message.extension_headers.subscribe.topic_name_length,
                    deserialized_message.extension_headers.subscribe.topic_name_length,
                );
                try testing.expect(std.mem.eql(u8, message.topicName(), deserialized_message.topicName()));
            },
            .auth_challenge => {
                try testing.expectEqual(
                    message.extension_headers.auth_challenge.challenge_method,
                    deserialized_message.extension_headers.auth_challenge.challenge_method,
                );
                try testing.expectEqual(
                    message.extension_headers.auth_challenge.algorithm,
                    deserialized_message.extension_headers.auth_challenge.algorithm,
                );
                try testing.expectEqual(
                    message.extension_headers.auth_challenge.nonce,
                    deserialized_message.extension_headers.auth_challenge.nonce,
                );
                try testing.expectEqual(
                    message.extension_headers.auth_challenge.connection_id,
                    deserialized_message.extension_headers.auth_challenge.connection_id,
                );
            },
            .session_init => {
                try testing.expectEqual(
                    message.extension_headers.session_init.peer_id,
                    deserialized_message.extension_headers.session_init.peer_id,
                );
                try testing.expectEqual(
                    message.extension_headers.session_init.peer_type,
                    deserialized_message.extension_headers.session_init.peer_type,
                );
            },
            .session_join => {
                try testing.expectEqual(
                    message.extension_headers.session_join.peer_id,
                    deserialized_message.extension_headers.session_join.peer_id,
                );
                try testing.expectEqual(
                    message.extension_headers.session_join.session_id,
                    deserialized_message.extension_headers.session_join.session_id,
                );
            },
            .auth_failure => {
                try testing.expectEqual(
                    message.extension_headers.auth_failure.error_code,
                    deserialized_message.extension_headers.auth_failure.error_code,
                );
            },
            .auth_success => {
                try testing.expectEqual(
                    message.extension_headers.auth_success.peer_id,
                    deserialized_message.extension_headers.auth_success.peer_id,
                );
                try testing.expectEqual(
                    message.extension_headers.auth_success.session_id,
                    deserialized_message.extension_headers.auth_success.session_id,
                );
            },
        }
    }
}
