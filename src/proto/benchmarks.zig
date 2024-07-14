// TODO: Fix the imports
const std = @import("std");
const Message = @import("./message.zig").Message;
const MessageParser = @import("./parser.zig").MessageParser;
const serialize = @import("./utils.zig").serialize;

fn average(elems: []u64) u64 {
    if (elems.len == 0) return 0;

    var sum: u64 = 0;
    for (elems) |e| {
        sum += e;
    }

    return sum / elems.len;
}

fn max(elems: []u64) u64 {
    if (elems.len == 0) return 0;

    var m: u64 = 0;
    for (elems) |e| {
        if (m == 0) {
            m = e;
            continue;
        }
        if (e > m) {
            m = e;
        }
    }

    return m;
}

fn min(elems: []u64) u64 {
    if (elems.len == 0) return 0;

    var m: u64 = 0;
    for (elems) |e| {
        if (m == 0) {
            m = e;
            continue;
        }

        if (e < m) {
            m = e;
        }
    }

    return m;
}

// TODO: Convert this to use Zbench once possible
// test "primitive benchmark message serialization" {
//     const ITERS: u32 = 500;
//     // Create an empty default message on the stack
//     const msg = Message.new("", "", "");
//
//     var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
//     defer arena.deinit();
//     const arena_allocator = arena.allocator();
//
//     var iter_durations = std.ArrayList(u64).init(arena_allocator);
//     defer iter_durations.deinit();
//
//     /////////////////// SERIALIZE THE MESSAGE
//     var serialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const serialize_buf_allocator = serialize_buf_gpa.allocator();
//     defer _ = serialize_buf_gpa.deinit();
//
//     var serialize_buf = std.ArrayList(u8).init(serialize_buf_allocator);
//     defer serialize_buf.deinit();
//
//     for (0..ITERS) |_| {
//         var serialize_timer = try std.time.Timer.start();
//
//         // DO THE WORK
//         try serialize(&serialize_buf, msg);
//
//         const serialize_duration = serialize_timer.read();
//         try iter_durations.append(serialize_duration);
//         serialize_timer.reset();
//         serialize_buf.clearAndFree();
//     }
//
//
//     try std.testing.expect(average(iter_durations.items) / std.time.ns_per_us < 50);
// }

// test "primitive benchmark message parse" {
//
//     // TODO: the parser needs to likely be redesigned.
//     //       the main issue that i'm finding is that allocating/deallocating memory
//     //       results in leaks. I'm thinking it has something to do with the buffer
//     //       ArrayList(u8) within the parser
//     //
//     //       ideally, i'd want to have the following flow
//     //
//     //       p = MessageParser.init(allocator)
//     //       defer p.deinit();
//     //
//     //       try p.parse(output, input)
//     //
//     // const ITERS: u32 = 500;
//     //
//     // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
//     // defer arena.deinit();
//     // const arena_allocator = arena.allocator();
//     //
//     // var iter_durations = std.ArrayList(u64).init(arena_allocator);
//     // defer iter_durations.deinit();
//     //
//     // const bytes: []const u8 = &.{ 0, 0, 0, 18, 165, 0, 96, 1, 0, 2, 96, 3, 96, 103, 104, 101, 97, 100, 101, 114, 115, 160 };
//     //
//     // // create the buffer where the messages will be output
//     // var parsed_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     // const parsed_messages_allocator = parsed_messages_gpa.allocator();
//     // defer _ = parsed_messages_gpa.deinit();
//     //
//     // var parsed_messages = std.ArrayList([]u8).init(parsed_messages_allocator);
//     // defer parsed_messages.deinit();
//     //
//     // // create the parser
//     // var parser_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
//     // const parser_allocator = parser_arena.allocator();
//     // // defer parser_arena.deinit();
//     //
//     // var parser = MessageParser.init(parser_allocator);
//     // // defer parser.deinit();
//     //
//     // for (0..ITERS) |_| {
//     //     var serialize_timer = try std.time.Timer.start();
//     //
//     //     // DO THE WORK
//     //     try parser.parse(&parsed_messages, bytes);
//     //
//     //     const serialize_duration = serialize_timer.read();
//     //     serialize_timer.reset();
//     //
//     //     try iter_durations.append(serialize_duration);
//     // }
//     //
//     // try std.testing.expect(average(iter_durations.items) / std.time.ns_per_us < 20);
// }
