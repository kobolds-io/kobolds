const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const Message = @import("./message.zig").Message;
const FixedHeaders = @import("./message.zig").FixedHeaders;
const ExtensionHeaders = @import("./message.zig").ExtensionHeaders;
const Chunk = @import("./chunk.zig").Chunk;
const ChunkReader = @import("./chunk.zig").ChunkReader;
const ChunkWriter = @import("./chunk.zig").ChunkWriter;

const MemoryPool = @import("stdx").MemoryPool;

pub const Parser = struct {
    const Self = @This();

    reader: ChunkReader,
    scratch: [128]u8 = undefined,

    /// Make a new `Parser` which parses a single message from a `chunk_chain`. The `Parser` assumes
    /// that a `chunk_chain` only contains a single message.
    pub fn new(head_chunk: *Chunk) Self {
        return Self{
            .reader = ChunkReader.new(head_chunk),
            .scratch = undefined,
        };
    }

    pub fn parse(self: *Self) !Message {
        // Try to read the FixedHeaders out of the chunk
        const fh_size = FixedHeaders.packedSize();
        const fixed_slice = try self.reader.readExactSlice(fh_size, &self.scratch);
        const fixed_headers = try FixedHeaders.fromBytes(fixed_slice);

        // Try to read the ExtensionHeaders out of the chunk
        const eh_size = ExtensionHeaders.packedSize(fixed_headers.message_type);
        const eh_slice = try self.reader.readExactSlice(eh_size, &self.scratch);
        const extension_headers = try ExtensionHeaders.fromBytes(
            fixed_headers.message_type,
            eh_slice,
        );

        // everything else remaining in this chunk chain should be considered as part of the body.
        // The parser does not use the `Message.init` function because everything has already been
        // allocated. Messages should never be reallocated, only referenced
        return Message{
            .fixed_headers = fixed_headers,
            .extension_headers = extension_headers,
            .chunk = self.reader.current orelse unreachable,
        };
    }
};

test "parser parses a message from valid chunk" {
    const allocator = testing.allocator;

    var fixed_headers_before = FixedHeaders{
        .message_type = .ping,
    };

    var extension_headers_before = ExtensionHeaders{
        .ping = .{
            .transaction_id = 100,
        },
    };

    var message_buf: [100]u8 = undefined;
    var n = fixed_headers_before.toBytes(message_buf[0..]);
    n += extension_headers_before.toBytes(message_buf[n..]);

    var pool = try MemoryPool(Chunk).init(allocator, 5);
    defer pool.deinit();

    const chunk = try pool.create();
    defer pool.destroy(chunk);

    chunk.* = Chunk{};

    var writer = ChunkWriter.new(chunk);
    try writer.write(&pool, message_buf[0..n]);

    var parser = Parser.new(chunk);

    // we don't have to deinit this message because we are initializing the chunk above. Additionally
    // we are not going to increment the `Message.ref_count` for this message since no other
    // thread is trying to access it.
    const message = try parser.parse();

    try testing.expectEqual(fixed_headers_before.flags.padding, message.fixed_headers.flags.padding);
    try testing.expectEqual(fixed_headers_before.message_type, message.fixed_headers.message_type);
    try testing.expectEqual(extension_headers_before.ping.transaction_id, message.extension_headers.ping.transaction_id);
}

// parser should basically walk the chunks and figure out the validity of the message.
// Since the message fixed headers and extension headers should ALWAYS fit into a single chunk
// we can assume that the rest of the message is simply the body. So, to me that kind of means
// that we can rely on the first chunk containing all the information about the rest of the message.
//
// Since this is what a message looks like
// [ fixed_headers | extension_headers | body ]
//
// The message would really only need to store the following
// 1. fixed_headers stuff
// 2. extensions_headers stuff
// 4. first chunk ref
//
// I can then make some helpers to help access the body more dynamically. A message is guaranteed
// to have a first chunk. I think a message should also be ref counted which will help me figure
// out when to deallocate the message.
//
//
// Message {
//  fixed_headers,
//  extension_headers,
//  chunk,
// }
//
// var message = Message.init(pool, message_type, );
// defer message.deinit();
//
// message.ref();
// message.deref();
