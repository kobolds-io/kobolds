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

    head_chunk: *Chunk,

    pub fn new(head_chunk: *Chunk) Self {
        return Self{
            .head_chunk = head_chunk,
        };
    }

    pub fn parse(self: *Self) !*Message {
        var reader = ChunkReader.new(self.head_chunk);

        // Try to read the FixedHeaders out of the chunk
        var scratch: [32]u8 = undefined;
        const fixed_headers_slice = try reader.readExactSlice(FixedHeaders.packedSize(), &scratch);
        const fixed_headers = try FixedHeaders.fromBytes(fixed_headers_slice);

        const eh_size = switch (fixed_headers.message_type) {
            .ping => blk: {
                const headers = ExtensionHeaders{ .ping = .{} };
                break :blk headers.packedSize();
            },
            .pong => blk: {
                const headers = ExtensionHeaders{ .pong = .{} };
                break :blk headers.packedSize();
            },
            else => unreachable,
        };

        const extension_headers_slice = try reader.readExactSlice(eh_size, &scratch);
        const extension_headers = try ExtensionHeaders.fromBytes(fixed_headers.message_type, extension_headers_slice);

        // const fixed_headers = try FixedHeaders.parse(&self.reader);

        std.debug.print("fixed_headers: {any}\n", .{fixed_headers});
        std.debug.print("extension_headers: {any}\n", .{extension_headers});

        return error.NotImplemented;

        // return error.NotImplemented;

        // var reader = ChunkReader.new(chunk);
        // var count: usize = 0;

        // while (count < messages.len) {
        // const fixed_headers = try FixedHeaders.parse(&reader);
        // }
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
    _ = try parser.parse();

    // std.debug.print("chunk: {any}\n", .{chunk.data});
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
// var message = Message.new(head_chunk);
// defer message.deinit();
//
// message.ref();
// message.deref();
