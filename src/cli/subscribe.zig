const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const constants = @import("../constants.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const Message = @import("../protocol/message.zig").Message;

pub fn SubscribeCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                             Display this help and exit.
        \\-H, --host        <host>               Node host (default 127.0.0.1)
        \\-p, --port        <port>               Node port (default 8000)
        \\-i, --client-id   <client_id>          id of the client (default: 1) 
        \\-t, --token       <token>              Authentication token (default: ""),
        \\--max-connections <max_connections>    Maximum number of connections to open (default: 1)
        \\--min-connections <min_connections>    Minimum number of connections to open (default: 1)
        \\<topic_name>                           Topic name
    );

    const subscribe_parsers = .{
        .client_id = clap.parsers.int(u11, 10),
        .host = clap.parsers.string,
        .max_connections = clap.parsers.int(u16, 10),
        .min_connections = clap.parsers.int(u16, 10),
        .port = clap.parsers.int(u16, 10),
        .token = clap.parsers.string,
        .topic_name = clap.parsers.string,
    };

    // Here we pass the partially parsed argument iterator.
    var diag = clap.Diagnostic{};
    var parsed_args = clap.parseEx(clap.Help, &params, subscribe_parsers, iter, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err; // propagate error
    };
    defer parsed_args.deinit();

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    if (parsed_args.positionals.len != 1) return error.MissingParams;

    const args = SubscribeArgs{
        .client_id = parsed_args.args.@"client-id" orelse 1,
        .host = parsed_args.args.host orelse "127.0.0.1",
        .max_connections = parsed_args.args.@"max-connections" orelse 1,
        .min_connections = parsed_args.args.@"min-connections" orelse 1,
        .port = parsed_args.args.port orelse 8000,
        .token = parsed_args.args.token orelse "",
        .topic_name = parsed_args.positionals[0].?,
    };

    try subscribe(args);
}

const SubscribeArgs = struct {
    client_id: u11,
    host: []const u8,
    max_connections: u16,
    min_connections: u16,
    port: u16,
    token: []const u8,
    topic_name: []const u8,
};

var messages_recv_count: u128 = 0;

fn subscribe(args: SubscribeArgs) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const client_config = ClientConfig{
        .client_id = args.client_id,
        .host = args.host,
        .port = args.port,
        .max_connections = args.max_connections,
        .min_connections = args.min_connections,
        .authentication_config = .{
            .token_config = .{
                .id = args.client_id,
                .token = args.token,
            },
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    var timer = try std.time.Timer.start();
    const connect_start = timer.read();
    // wait for the client to be connected
    client.awaitConnected(5_000 * std.time.ns_per_ms);
    const connect_end = timer.read();

    std.debug.print("connection took {}ms\n", .{(connect_end - connect_start) / std.time.ns_per_ms});

    signal_handler.registerSigintHandler();

    const callback_1 = struct {
        pub fn callback(message: *Message) void {
            _ = message;
            messages_recv_count += 1;

            // std.debug.print("callback_1: messages_recv_count: {d}\n", .{messages_recv_count});
            if (messages_recv_count % 1000 == 0) {
                std.debug.print("callback_1: messages_recv_count: {d}\n", .{messages_recv_count});
            }
        }
    }.callback;

    // const callback_2 = struct {
    //     pub fn callback(message: *Message) void {
    //         messages_recv_count += 1;
    //         if (messages_recv_count % 1000 == 0) {
    //             const end = std.time.nanoTimestamp();
    //             const start = std.fmt.parseInt(i128, message.body(), 10) catch 0;

    //             const diff = @divFloor(end - start, std.time.ns_per_us);
    //             std.debug.print("messages_recv_count: {}, took: {d}us\n", .{ messages_recv_count, diff });
    //         }
    //     }
    // }.callback;
    // _ = callback_2;

    // const topic_name = "b" ** constants.message_max_topic_name_size;
    // const callback_1_id = try client.subscribe(topic_name, callback_1, .{});
    // defer client.unsubscribe(args.topic_name, callback_1_id, .{ .timeout_ms = 5_000 }) catch unreachable;

    const callback_2_id = try client.subscribe(args.topic_name, callback_1, .{});
    defer client.unsubscribe(args.topic_name, callback_2_id, .{ .timeout_ms = 5_000 }) catch unreachable;

    while (!signal_handler.sigint_triggered) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}
