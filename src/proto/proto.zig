// TODO: Fix the imports
const std = @import("std");
const Message = @import("./message.zig").Message;
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
test "primitive benchmark message serialization" {
    const ITERS: u32 = 100_000;
    // Create an empty default message on the stack
    const msg = Message.new("", "", "");

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var iter_durations = std.ArrayList(u64).init(arena_allocator);
    defer iter_durations.deinit();

    /////////////////// SERIALIZE THE MESSAGE
    var serialize_buf_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const serialize_buf_allocator = serialize_buf_gpa.allocator();
    defer _ = serialize_buf_gpa.deinit();

    var serialize_buf = std.ArrayList(u8).init(serialize_buf_allocator);
    defer serialize_buf.deinit();

    for (0..ITERS) |_| {
        var serialize_timer = try std.time.Timer.start();

        try serialize(&serialize_buf, msg);

        const serialize_duration = serialize_timer.read();
        try iter_durations.append(serialize_duration);
        serialize_timer.reset();
        serialize_buf.clearAndFree();
    }

    // std.debug.print(
    //     "average duration {d}\n",
    //     .{average(iter_durations.items) / std.time.ns_per_us},
    // );
    // std.debug.print(
    //     "min duration {d}\n",
    //     .{min(iter_durations.items) / std.time.ns_per_us},
    // );
    // std.debug.print(
    //     "max duration {d}\n",
    //     .{max(iter_durations.items) / std.time.ns_per_us},
    // );

    try std.testing.expect(average(iter_durations.items) / std.time.ns_per_us < 40);
    try std.testing.expect(min(iter_durations.items) / std.time.ns_per_us < 40);
    try std.testing.expect(max(iter_durations.items) / std.time.ns_per_us < 2000);
}
