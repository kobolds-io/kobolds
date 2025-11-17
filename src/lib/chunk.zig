const std = @import("std");
const testing = std.testing;

const constants = @import("../constants.zig");
const MemoryPool = @import("stdx").MemoryPool;

pub const Chunk = struct {
    used: usize = 0,
    data: [constants.chunk_data_size]u8 = undefined,
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

                if (self.current == null) return out_index; // EOF

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

    pub fn readByte(self: *ChunkReader) !u8 {
        // No current chunk -> nothing to read
        if (self.current == null) return error.EOF;

        var chunk = self.current.?;

        // If we've exhausted this chunk, advance to the next one
        if (self.offset >= chunk.used) {
            if (chunk.next == null) return error.EOF;

            // Move to next chunk
            self.current = chunk.next;
            chunk = chunk.next.?; // update local pointer
            self.offset = 0;

            if (chunk.used == 0) return error.EOF; // empty chunk with no data
        }

        const b = chunk.data[self.offset];
        self.offset += 1;
        return b;
    }

    /// Reads `n` bytes and returns them as a *direct slice into the chunk chain*
    /// if they are in a single chunk. Otherwise copies into `scratch`.
    pub fn readExactSlice(
        self: *Self,
        n: usize,
        scratch_buffer: []u8,
    ) ![]const u8 {
        if (self.current == null) return error.EOF;

        var remaining_bytes = n;
        var scratch_index: usize = 0;

        // If the entire read lives in one chunk, return direct slice
        const chunk = self.current.?;
        const available = chunk.used - self.offset;
        if (remaining_bytes <= available) {
            const slice = chunk.data[self.offset .. self.offset + remaining_bytes];
            self.offset += remaining_bytes;
            return slice;
        }

        // Otherwise fall back to copying
        if (scratch_buffer.len < n) return error.ScratchTooSmall;

        while (remaining_bytes > 0) {
            if (self.current == null) return error.EOF;

            const ch = self.current.?;
            const chunk_available = ch.used - self.offset;

            if (chunk_available == 0) {
                self.current = ch.next;
                self.offset = 0;
                continue;
            }

            const to_copy = @min(chunk_available, remaining_bytes);
            @memcpy(
                scratch_buffer[scratch_index .. scratch_index + to_copy],
                ch.data[self.offset .. self.offset + to_copy],
            );

            scratch_index += to_copy;
            self.offset += to_copy;
            remaining_bytes -= to_copy;
        }

        return scratch_buffer[0..n];
    }

    /// compute the remaining bytes in the chunk chain
    pub fn remaining(self: *Self) usize {
        if (self.current) |current| {
            var total_remaining: usize = 0;

            // count the bytes remaining in the current chunk
            const available = if (self.offset < current.used) current.used - self.offset else 0;
            total_remaining += available;

            // walk the remaining chunks in the chain
            var next = current.next;
            while (next) |chunk| {
                total_remaining += chunk.used;
                next = chunk.next;
            }

            return total_remaining;
        } else {
            return 0;
        }
    }
};

pub const ChunkWriter = struct {
    const Self = @This();

    head: *Chunk,
    current: *Chunk,
    offset: usize = 0,

    pub fn new(head: *Chunk) Self {
        return .{
            .head = head,
            .current = head,
            .offset = head.used,
        };
    }

    /// Internal helper: ensure `current` has space, and if not,
    /// move to or allocate the next chunk.
    fn ensureSpace(self: *Self, pool: *MemoryPool(Chunk), needed: usize) !void {
        const cap = constants.chunk_data_size;

        if (self.offset + needed <= cap) return;

        // Not enough space. Move to next chunk or allocate a new one.
        if (self.current.next) |next| {
            self.current = next;
            self.offset = self.current.used;
            return ensureSpace(self, pool, needed);
        }

        // Need to allocate a new chunk.
        const new_chunk = try pool.create();
        new_chunk.* = Chunk{};

        self.current.next = new_chunk;
        self.current = new_chunk;
        self.offset = 0;
    }

    /// Write a single byte
    pub fn writeByte(self: *Self, pool: *MemoryPool(Chunk), b: u8) !void {
        try self.ensureSpace(pool, 1);

        self.current.data[self.offset] = b;
        self.offset += 1;
        if (self.offset > self.current.used) self.current.used = self.offset;
    }

    /// Write arbitrary bytes into the chunk chain
    pub fn write(self: *Self, pool: *MemoryPool(Chunk), buf: []const u8) !void {
        var index: usize = 0;

        while (index < buf.len) {
            // Ensure at least 1 byte of space; chunk may rotate
            try self.ensureSpace(pool, 1);

            const cap = constants.chunk_data_size;
            const remaining = buf.len - index;
            const space = cap - self.offset;

            const to_copy = @min(remaining, space);

            @memcpy(
                self.current.data[self.offset .. self.offset + to_copy],
                buf[index .. index + to_copy],
            );

            self.offset += to_copy;
            if (self.offset > self.current.used)
                self.current.used = self.offset;

            index += to_copy;
        }
    }

    /// write a slice and return number of bytes written
    pub fn writeSlice(self: *Self, pool: *MemoryPool(Chunk), slice: []const u8) !usize {
        try self.write(pool, slice);
        return slice.len;
    }
};

test "chunk reader can read a chunk chain" {
    const allocator = testing.allocator;

    const c1_payload = try allocator.alloc(u8, constants.chunk_data_size);
    defer allocator.free(c1_payload);

    const c2_payload = try allocator.alloc(u8, constants.chunk_data_size);
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
    var buf: [constants.chunk_data_size / 4]u8 = undefined;
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
    var buf: [constants.chunk_data_size / 4]u8 = undefined;

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

        try std.testing.expectEqualSlices(u8, slice, expected[0..150]);
    }

    // Reading past end of the chunks give EOF
    const err = reader.readExactSlice(scratch.len, &scratch);
    try std.testing.expectError(error.EOF, err);
}

test "reader reads sequential bytes and errors at end" {
    var chunk1 = Chunk{};
    for (0..2) |i| {
        chunk1.data[i] = @intCast(i);
    }
    chunk1.used = 2;

    var chunk2 = Chunk{};
    for (0..1) |i| {
        chunk2.data[i] = @intCast(i);
    }
    chunk2.used = 1;

    // link the chunks together
    chunk1.next = &chunk2;

    var reader = ChunkReader.new(&chunk1);

    // First byte (chunk 1)
    try std.testing.expectEqual(0, try reader.readByte());

    // Second byte (chunk 1)
    try std.testing.expectEqual(1, try reader.readByte());

    // Third byte (chunk 2)
    try std.testing.expectEqual(0, try reader.readByte());

    // Now it should error
    const err = reader.readByte();
    try std.testing.expectError(error.EOF, err);
}

test "chunk writer can write arbitrary bytes" {
    const allocator = testing.allocator;

    var pool = try MemoryPool(Chunk).init(allocator, 10);
    defer pool.deinit();

    const c1 = try pool.create();
    c1.* = Chunk{};
    defer pool.destroy(c1);

    var writer = ChunkWriter.new(c1);

    var bytes: [constants.chunk_data_size / 2]u8 = undefined;

    var total_written: usize = 0;

    // write one time to the head chunk
    total_written += try writer.writeSlice(&pool, &bytes);
    try testing.expectEqual(pool.available(), pool.capacity - 1);
    try testing.expect(writer.current == c1);

    // write a second time, see the same first chunk is used
    total_written += try writer.writeSlice(&pool, &bytes);
    try testing.expectEqual(pool.available(), pool.capacity - 1);
    try testing.expect(writer.current == c1);

    // Write a thrid time. The pool should allocate a new chunk
    total_written += try writer.writeSlice(&pool, &bytes);
    try testing.expectEqual(pool.available(), pool.capacity - 2);
    try testing.expect(writer.current != c1);
    const c2 = writer.current;

    // Write a fourth time and ensure we are sill on the second chunk
    total_written += try writer.writeSlice(&pool, &bytes);
    try testing.expectEqual(pool.available(), pool.capacity - 2);
    try testing.expect(writer.current == c2);

    // ensure that all bytes have been written
    try testing.expectEqual(bytes.len * 4, total_written);

    try writer.writeByte(&pool, 10);
    try testing.expect(writer.current != c2);
}
