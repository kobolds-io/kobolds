const std = @import("std");
const testing = std.testing;

const constants = @import("../constants.zig");

pub const Chunk = struct {
    used: usize = 0,
    data: [constants.message_chunk_data_size]u8 = undefined,
    next: ?*Chunk = null,
};

pub const ChunkReader = struct {
    const Self = @This();

    current: ?*Chunk,
    offset: usize = 0,

    pub fn new(head: *Chunk) Self {
        return Self{
            .current = head,
            .offset = 0,
        };
    }

    /// Reads up to `buf.len` bytes into `buf`.
    pub fn read(self: *Self, buf: []u8) usize {
        if (self.current == null) return 0;

        var out_index: usize = 0;

        while (out_index < buf.len) {
            const chunk = self.current.?;
            if (self.offset >= chunk.used) {
                // move to next chunk
                self.current = chunk.next;
                self.offset = 0;

                if (self.current == null)
                    return out_index; // EOF

                continue;
            }

            const available = chunk.used - self.offset;
            const to_copy = @min(available, buf.len - out_index);

            @memcpy(
                buf[out_index .. out_index + to_copy],
                chunk.data[self.offset .. self.offset + to_copy],
            );

            self.offset += to_copy;
            out_index += to_copy;
        }

        return out_index;
    }

    /// Reads `n` bytes and returns them as a *direct slice into the chunk chain*
    /// if they are in a single chunk. Otherwise copies into `scratch`.
    pub fn readExactSlice(
        self: *Self,
        n: usize,
        scratch: []u8,
    ) ![]const u8 {
        if (self.current == null)
            return error.UnexpectedEOF;

        var remaining = n;
        var scratch_index: usize = 0;

        // If the entire read lives in one chunk, return direct slice
        const chunk = self.current.?;
        const available = chunk.used - self.offset;
        if (remaining <= available) {
            const slice = chunk.data[self.offset .. self.offset + remaining];
            self.offset += remaining;
            return slice;
        }

        // Otherwise fall back to copying
        if (scratch.len < n)
            return error.ScratchTooSmall;

        while (remaining > 0) {
            if (self.current == null)
                return error.UnexpectedEOF;

            const ch = self.current.?;
            const ch_avail = ch.used - self.offset;

            if (ch_avail == 0) {
                self.current = ch.next;
                self.offset = 0;
                continue;
            }

            const to_copy = @min(ch_avail, remaining);
            @memcpy(
                scratch[scratch_index .. scratch_index + to_copy],
                ch.data[self.offset .. self.offset + to_copy],
            );

            scratch_index += to_copy;
            self.offset += to_copy;
            remaining -= to_copy;
        }

        return scratch[0..n];
    }
};

test "chunk reader can read a chunk chain" {
    const allocator = testing.allocator;

    const c1_payload = try allocator.alloc(u8, constants.message_chunk_data_size);
    defer allocator.free(c1_payload);

    const c2_payload = try allocator.alloc(u8, constants.message_chunk_data_size);
    defer allocator.free(c2_payload);

    for (0..c1_payload.len) |i| {
        c1_payload[i] = 1;
    }

    for (0..c2_payload.len) |i| {
        c2_payload[i] = 2;
    }

    var c1 = Chunk{};
    @memcpy(c1.data[0..], c1_payload);
    c1.used = c1_payload.len;

    var c2 = Chunk{};
    @memcpy(c2.data[0..], c2_payload);
    c2.used = c2_payload.len;

    // chain the chunks together
    c1.next = &c2;

    // we now have a chain of chunks
    var reader = ChunkReader.new(&c1);

    // make a buffer that is smaller than the size of each chunk
    var buf: [constants.message_chunk_data_size / 4]u8 = undefined;
    var bytes_read: usize = 0;
    const total_bytes = c1.used + c2.used;

    var i: usize = 1;
    while (true) : (i += 1) {
        const n = reader.read(&buf);
        bytes_read += n;

        if (bytes_read == total_bytes) {
            // ensure that we have iterated the expected number of times
            try testing.expectEqual(i, total_bytes / buf.len);
            break;
        } else {
            // ensure we are reading the entire size of the buffer every time
            try testing.expectEqual(n, buf.len);
        }
    }
}

test "chunk reader can read small chunks" {
    const allocator = testing.allocator;

    const c1_payload = try allocator.alloc(u8, 10);
    defer allocator.free(c1_payload);

    for (0..c1_payload.len) |i| {
        c1_payload[i] = 1;
    }

    var c1 = Chunk{};
    @memcpy(c1.data[0..c1_payload.len], c1_payload);
    c1.used = c1_payload.len;

    // we now have a chain of chunks
    var reader = ChunkReader.new(&c1);

    // make a buffer that is smaller than the size of each chunk
    var buf: [constants.message_chunk_data_size / 4]u8 = undefined;

    const n = reader.read(&buf);
    try testing.expectEqual(n, c1.used);
}
