const std = @import("std");
const net = std.net;
const Node = @import("./proto/node.zig").Node;
const MessageParser = @import("./proto/parser.zig").MessageParser;

const BufferSize = 1024;

// fn handleClient(client_stream: *std.net.Stream, allocator: *std.mem.Allocator) !void {
//     var buffer: [BufferSize]u8 = undefined;
//     while (true) {
//         const read_bytes = try client_stream.reader().read(buffer[0..]);
//         if (read_bytes == 0) break; // Client closed the connection
//         try client_stream.writer().writeAll(buffer[0..read_bytes]);
//     }
// }

pub fn main() !void {
    // var inbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const inbox_allocator = inbox_gpa.allocator();
    // defer _ = inbox_gpa.deinit();
    //
    // var outbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const outbox_allocator = outbox_gpa.allocator();
    // defer _ = outbox_gpa.deinit();
    //
    // var connections_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const connections_allocator = connections_gpa.allocator();
    // defer _ = connections_gpa.deinit();
    //
    // const node = Node.new(inbox_allocator, outbox_allocator, connections_allocator);

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();

    const address = try net.Address.parseIp("127.0.0.1", 3000);
    var listener = try address.listen(.{
        .reuse_address = true,
        .kernel_backlog = 1024,
    });
    defer listener.deinit();
    std.log.info("listening at {any}\n", .{address});

    // const node = Node.new_shared(allocator);
    // var room = Room{ .lock = .{}, .clients = std.AutoHashMap(*Client, void).init(allocator) };

    while (true) {
        var conn = listener.accept() catch |err| {
            std.debug.print("could not accept connection {}", .{err});
            continue;
        };

        const t = [_]u8{ '1', '2', '3' };
        _ = try conn.stream.write(&t);

        conn.stream.close();

        // if (listener.accept()) |conn| {
        //     var client_arena = ArenaAllocator.init(allocator);
        //     const client = try client_arena.allocator().create(Client);
        //     errdefer client_arena.deinit();
        //
        //     client.* = Client.init(client_arena, conn.stream, &room);
        //
        //     const thread = try std.Thread.spawn(.{}, Client.run, .{client});
        //     thread.detach();
        // } else |err| {
        //     std.log.err("failed to accept connection {}", .{err});
        // }
    }

    // TODO: parse a config file to populate env variables

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // errdefer _ = gpa.deinit();
    //
    // var parser = MessageParser.init(allocator);
    //
    // // 05hello07hellooo
    // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 7, 104, 101, 108, 108, 111, 111, 111 };
    // // 05hello
    // // const bytes = [_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    // var timer = try std.time.Timer.start();
    //
    // const iters = 1_000_000;
    // std.debug.print("sending {} bytes through parser {} times\n", .{ bytes.len, iters });
    // for (0..iters) |_| {
    //     _ = try parser.parse(&bytes);
    // }
    //
    // const took = timer.read();
    // std.debug.print("took {}ns\n", .{took});
    // std.debug.print("took {}us\n", .{took / std.time.ns_per_us});
    // std.debug.print("took {}ms\n", .{took / std.time.ns_per_ms});
}
