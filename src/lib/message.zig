const std = @import("std");
const atomic = std.atomic;
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const MemoryPool = @import("stdx").MemoryPool;

const MessageType = @import("./message_type.zig").MessageType;
const PingHeaders = @import("./headers.zig").PingHeaders;
const PongHeaders = @import("./headers.zig").PongHeaders;

const ChunkReader = @import("./chunk.zig").ChunkReader;
const ChunkWriter = @import("./chunk.zig").ChunkWriter;
const Chunk = @import("./chunk.zig").Chunk;

comptime {
    // NOTE: A core assumption of the message is that the FixedHeaders and ExtensionsHeaders will be
    // able to fit inside of a single chunk. If this no longer is the case, then the entire
    // message structure should be refactored to comply with the new restrictions
    assert(constants.chunk_data_size >= FixedHeaders.packedSize() + @sizeOf(ExtensionHeaders));
}

pub const Message = struct {
    const Self = @This();

    pub const Options = struct {};

    fixed_headers: FixedHeaders = FixedHeaders{},
    extension_headers: ExtensionHeaders = ExtensionHeaders{ .unsupported = {} },
    chunk: *Chunk,
    ref_count: atomic.Value(usize) = atomic.Value(usize).init(0),

    pub fn init(pool: *MemoryPool(Chunk), message_type: MessageType, _: Options) !Message {
        // create a head chunk
        const chunk = try pool.create();
        errdefer pool.destroy(chunk);

        chunk.* = Chunk{};

        // initialize a writer
        var writer = ChunkWriter.new(chunk);

        var fixed_headers = FixedHeaders{ .message_type = message_type };

        var tmp_buf: [FixedHeaders.packedSize() + @sizeOf(ExtensionHeaders)]u8 = undefined;
        const fh_n = fixed_headers.toBytes(tmp_buf[0..]);

        // FIX: this is trash and should be a function of some type
        var extension_headers = switch (message_type) {
            .unsupported => ExtensionHeaders{ .unsupported = {} },
            .ping => ExtensionHeaders{ .ping = .{} },
            .pong => ExtensionHeaders{ .pong = .{} },
        };

        const eh_n = extension_headers.toBytes(tmp_buf[fh_n..]);

        try writer.write(pool, tmp_buf[0 .. fh_n + eh_n]);

        return Message{
            .fixed_headers = fixed_headers,
            .extension_headers = extension_headers,
            .chunk = chunk,
        };
    }

    pub fn deinit(self: *Self, pool: *MemoryPool(Chunk)) void {
        // recursively destroy the chunk chain
        var current_chunk: ?*Chunk = self.chunk;
        var next_chunk: ?*Chunk = null;
        while (current_chunk) |chunk| {
            next_chunk = chunk.next;
            pool.destroy(chunk);
            current_chunk = next_chunk;
        }
    }

    fn headersPackedSize(self: Self) usize {
        const fh_size = FixedHeaders.packedSize();
        const eh_size = ExtensionHeaders.packedSize(self.fixed_headers.message_type);
        return fh_size + eh_size;
    }

    pub fn packedSize(self: Self) usize {
        var reader = ChunkReader.new(self.chunk);
        return reader.remaining();
    }

    pub fn bodySize(self: Self) usize {
        var reader = ChunkReader{
            .offset = self.headersPackedSize(),
            .current = self.chunk,
        };

        return reader.remaining();
    }

    pub fn bodyReader(self: Self) ChunkReader {
        return ChunkReader{
            .offset = self.headersPackedSize(),
            .current = self.chunk,
        };
    }

    pub fn bodyWriter(self: Self) ChunkWriter {
        return ChunkWriter{
            .head = self.chunk,
            .current = self.chunk,
            .offset = self.headersPackedSize(),
        };
    }

    pub fn setBody(self: Self, pool: *MemoryPool(Chunk), v: []const u8) !void {
        // Reset head chunk to only contain the headers.
        self.chunk.used = self.headersPackedSize();

        // Traverse and clear all following chunks.
        var chunk_opt = self.chunk.next;
        while (chunk_opt) |chunk| {
            chunk.used = 0;
            chunk_opt = chunk.next;
        }

        // Write new body bytes into the chunk chain.
        var writer = self.bodyWriter();
        try writer.write(pool, v);

        // Now recursively clean up unused chunks. Keep pointer to the first chunk after head.
        var prev: *Chunk = self.chunk;
        var current_opt = prev.next;

        while (current_opt) |current| {
            const next = current.next;

            if (current.used == 0) {
                // unlink current
                prev.next = next;

                // destroy safely
                pool.destroy(current);
            } else {
                // Only advance prev if this chunk stays alive
                prev = current;
            }

            current_opt = next;
        }
    }

    /// return the current `ref_count` for this message
    pub fn refs(self: *Self) u32 {
        return self.ref_count.load(.seq_cst);
    }

    /// increment the `ref_count` for this message
    pub fn ref(self: *Self) void {
        _ = self.ref_count.fetchAdd(1, .seq_cst);
    }

    /// decrement the `ref_count` for this message. `Message.ref_count` should never be less than
    /// zero and therefore will panic due to logical error if that is the case.
    pub fn deref(self: *Self) void {
        // this is a logical guard as we should never be dereferencing messages more than we
        // have previously referenced them
        assert(self.refs() > 0);

        _ = self.ref_count.fetchSub(1, .seq_cst);
    }
};

pub const Flags = packed struct {
    const Self = @This();

    padding: u8 = 0,

    pub fn packedSize() usize {
        return 1;
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        assert(buf.len >= Flags.packedSize());

        var i: usize = 0;
        buf[i] = self.padding;
        i += 1;

        return i;
    }
};

pub const FixedHeaders = struct {
    const Self = @This();

    comptime {
        assert(2 == FixedHeaders.packedSize());
    }

    message_type: MessageType = .unsupported,
    flags: Flags = Flags{},

    pub fn packedSize() usize {
        return @sizeOf(MessageType) + @sizeOf(Flags);
    }

    pub fn toBytes(self: *Self, buf: []u8) usize {
        assert(buf.len >= @sizeOf(Self));

        var i: usize = 0;

        buf[i] = @intFromEnum(self.message_type);
        i += @sizeOf(MessageType);

        const flag_bytes = self.flags.toBytes(buf[i .. i + @sizeOf(Flags)]);
        i += flag_bytes;

        return i;
    }

    pub fn fromBytes(data: []const u8) !Self {
        if (data.len < Self.packedSize()) return error.Truncated;

        var i: usize = 0;

        const message_type = MessageType.fromByte(data[i]);
        if (message_type == .unsupported) return error.InvalidMessageType;
        i += 1;

        // get the bits for the flags
        const flags: Flags = @bitCast(data[i]);
        i += 1;

        return FixedHeaders{
            .message_type = message_type,
            .flags = flags,
        };
    }
};

pub const ExtensionHeaders = union(MessageType) {
    const Self = @This();

    unsupported: void,
    ping: PingHeaders,
    pong: PongHeaders,

    pub fn packedSize(message_type: MessageType) usize {
        return switch (message_type) {
            .unsupported => 0,
            .ping => PingHeaders.packedSize(),
            .pong => PongHeaders.packedSize(),
        };
    }

    pub fn toBytes(self: Self, buf: []u8) usize {
        return switch (self) {
            .unsupported => 0,
            inline else => |headers| blk: {
                assert(buf.len >= @sizeOf(@TypeOf(headers)));

                const bytes = headers.toBytes(buf);
                break :blk bytes;
            },
        };
    }

    pub fn fromBytes(message_type: MessageType, data: []const u8) !Self {
        return switch (message_type) {
            .unsupported => ExtensionHeaders{ .unsupported = {} },
            .ping => blk: {
                const headers = try PingHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .ping = headers };
            },
            .pong => blk: {
                const headers = try PongHeaders.fromBytes(data);
                break :blk ExtensionHeaders{ .pong = headers };
            },
        };
    }
};

test "core message functionalities" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 3);
    defer pool.deinit();

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    try testing.expectEqual(message.chunk.used, message.packedSize());

    var body_writer = message.bodyWriter();

    const new_body = "kobolds is a friendly message broker...sneaky sneaky";
    try body_writer.write(&pool, new_body);

    try testing.expectEqual(message.chunk.used, message.packedSize());

    var body_reader = message.bodyReader();
    var body_reader_buf: [new_body.len]u8 = undefined;

    const n = body_reader.read(&body_reader_buf);

    try testing.expectEqual(new_body.len, n);

    try testing.expect(std.mem.eql(u8, body_reader_buf[0..n], new_body));
}

test "message can expand and contract based on needs" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 3);
    defer pool.deinit();

    var message = try Message.init(&pool, .ping, .{});
    defer message.deinit(&pool);

    try testing.expectEqual(1, pool.capacity - pool.available());

    // sanity check
    try testing.expectEqual(message.chunk.used, message.packedSize());
    try testing.expectEqual(0, message.bodySize());

    const bytes = [_]u8{'a'} ** constants.chunk_data_size;

    try message.setBody(&pool, &bytes);

    // because the size of the body has now exceeded the capacity of a single chunk
    // we should now have 2 chunks allocated by the memory pool
    try testing.expect(message.chunk.used < message.packedSize());
    try testing.expectEqual(constants.chunk_data_size, message.bodySize());
    try testing.expectEqual(2, pool.capacity - pool.available());

    // clear the message body
    try message.setBody(&pool, "");
    try testing.expectEqual(0, message.bodySize());
    try testing.expectEqual(1, pool.capacity - pool.available());

    // everything should now fit into a single chunk
    try testing.expectEqual(message.chunk.used, message.packedSize());
}

// NOTE: this is commented out because Message.fixed_headers and Message.extension_headers
// are just copies of the underlying chunk bytes. Since they are so small, this is OK. in an ideal
// world we would have easy wrapper functions that would read the underlying chunk's bytes and
// cast them into the types that we care about. I'm leaving this test for now but I can be convinced
// either way.
// test "changing the values of the headers directly changes the backing chunk" {
//     const allocator = testing.allocator;

//     var pool = try MemoryPool(Chunk).init(allocator, 3);
//     defer pool.deinit();

//     var message = try Message.init(&pool, .ping, .{});
//     defer message.deinit(&pool);

//     var f = message.fixedHeaders();

//     // message type
//     try testing.expectEqual(@intFromEnum(f.message_type), message.chunk.data[0]);
//     try testing.expectEqual(0, message.chunk.data[1]);

//     message.chunk.data[0] = 2;

//     f = message.fixedHeaders();

//     // message type
//     try testing.expectEqual(@intFromEnum(f.message_type), message.chunk.data[0]);
//     try testing.expectEqual(0, message.chunk.data[1]);
// }
