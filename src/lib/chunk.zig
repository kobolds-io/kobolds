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
        scratch_buffer: []u8,
    ) ![]const u8 {
        if (self.current == null) return error.UnexpectedEOF;

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
        if (scratch_buffer.len < n) return error.ScratchTooSmall;

        while (remaining > 0) {
            if (self.current == null) return error.UnexpectedEOF;

            const ch = self.current.?;
            const chunk_available = ch.used - self.offset;

            if (chunk_available == 0) {
                self.current = ch.next;
                self.offset = 0;
                continue;
            }

            const to_copy = @min(chunk_available, remaining);
            @memcpy(
                scratch_buffer[scratch_index .. scratch_index + to_copy],
                ch.data[self.offset .. self.offset + to_copy],
            );

            scratch_index += to_copy;
            self.offset += to_copy;
            remaining -= to_copy;
        }

        return scratch_buffer[0..n];
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

test "chunk can read exactly bytes and handle single and multi-chunk reads" {
    var chunk1 = Chunk{};
    for (0..100) |i| {
        chunk1.data[i] = @intCast(i);
    }
    chunk1.used = 100;

    var chunk2 = Chunk{};
    for (100..200) |i| {
        chunk2.data[i - 100] = @intCast(i);
    }
    chunk2.used = 100;

    // link the chunks together
    chunk1.next = &chunk2;

    var reader = ChunkReader.new(&chunk1);

    // Scratch buffer for slow-path reads
    var scratch: [256]u8 = undefined;

    // Fast path read entirely within chunk1

    {
        const slice = try reader.readExactSlice(10, &scratch);

        // const expected_ptr: [*]const u8 = &chunk1.data[0];
        // try std.testing.expect(slice.ptr == expected_ptr);

        // try std.testing.expect(slice.ptr == &chunk1.data[0]); // direct slice into chunk
        // data was saved directly to the slice
        try std.testing.expectEqualSlices(u8, slice, chunk1.data[0..10]);
    }

    // crossing chunk boundary forces memcpy into scratch buffer
    {
        // position is now offset 10 in chunk1; reading 200 requires going into chunk2
        const slice = try reader.readExactSlice(150, &scratch);

        // The slice that is returned is actually derrived from the scratch buffer

        // Validate correctness
        var expected: [150]u8 = undefined;

        // first 90 bytes from chunk1 (10..100)
        @memcpy(expected[0..90], chunk1.data[10..100]);

        // next 60 bytes from chunk2 (0..60)
        @memcpy(expected[90..150], chunk2.data[0..60]);

        // std.debug.print("slice: {any}\n", .{slice});
        // std.debug.print("scratch: {any}\n", .{scratch});

        try std.testing.expectEqualSlices(u8, slice, expected[0..150]);
    }

    // Reading past end of the chunks give UnexpectedEOF
    const err = reader.readExactSlice(scratch.len, &scratch);
    try std.testing.expectError(error.UnexpectedEOF, err);
}
