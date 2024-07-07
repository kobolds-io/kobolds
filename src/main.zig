const std = @import("std");
const net = std.net;
const Node = @import("./proto/node.zig").Node;
const MessageParser = @import("./proto/parser.zig").MessageParser;

pub fn main() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();
    //
    // const address = try net.Address.parseIp("127.0.0.1", 4000);
    // var listener = try address.listen(.{
    //     .reuse_address = true,
    //     .kernel_backlog = 1024,
    // });
    // defer listener.deinit();
    // std.log.info("listening at {any}\n", .{address});
    //
    // var nh = Node.new(allocator);
    //
    // // thread pool time!
    // var pool: std.Thread.Pool = undefined;
    // // I added 32 jobs to spawn 32 worker threads but idk, likely can do more
    // try pool.init(.{ .allocator = allocator, .n_jobs = 32 });
    // defer pool.deinit();
    //
    // while (true) {
    //     var conn = listener.accept() catch |err| {
    //         std.debug.print("could not accept connection {}", .{err});
    //         continue;
    //     };
    //
    //     // _ = try node.add_connection(conn);
    //
    //     try pool.spawn(Node.handle_connection, .{ &nh, &conn });
    // }

    // TODO: parse a config file to populate env variables

    var buffer_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const buffer_allocator = buffer_gpa.allocator();
    defer _ = buffer_gpa.deinit();
    var parser = MessageParser.init(buffer_allocator);

    var parsed_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const parsed_messages_allocator = parsed_messages_gpa.allocator();
    defer _ = parsed_messages_gpa.deinit();

    var parsed_messages = std.ArrayList([]u8).init(parsed_messages_allocator);
    defer _ = parsed_messages.deinit();

    // 05hello
    const data = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    try parser.parse(&parsed_messages, &data);

    std.debug.print("parsed_messages.items {any}\n", .{parsed_messages.items});

    // var messages = std.ArrayList([]const u8).init(messages_allocator);
    // defer messages.deinit();
    //
    // // var parser = MessageParser.new();
    //
    // // 05hello07hellooo
    // // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    // // 05hello
    // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    // var timer = try std.time.Timer.start();
    //
    // const iters = 1;
    // std.debug.print("sending {} bytes through parser {} times\n", .{ bytes.len, iters });
    //
    // var buffer: [1024]u8 = undefined;
    // var end_idx: usize = 0;
    //
    // for (0..iters) |_| {
    //     // std.debug.print("i {d}\n", .{i});
    //     // std.time.sleep(std.time.ns_per_ms * 1);
    //     // _ = try parser.parse(&messages, &bytes);
    //     _ = try MessageParser.parse(&messages, &bytes, &buffer, &end_idx);
    // }

    // std.debug.print("messages {any}\n", .{messages.items});
    //
    // const took = timer.read();
    // std.debug.print("took {}ns\n", .{took});
    // std.debug.print("took {}us\n", .{took / std.time.ns_per_us});
    // std.debug.print("took {}ms\n", .{took / std.time.ns_per_ms});
}
