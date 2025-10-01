const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const constants = @import("../constants.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;
const Message = @import("../protocol/message2.zig").Message;

pub fn AdvertiseCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

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
        \\<body>                                 Body of the message
    );

    const advertise_parsers = .{
        .body = clap.parsers.string,
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
    var parsed_args = clap.parseEx(clap.Help, &params, advertise_parsers, iter, .{
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

    if (parsed_args.positionals.len != 2) return error.MissingParams;

    const args = AdvertiseArgs{
        .body = parsed_args.positionals[1].?,
        .client_id = parsed_args.args.@"client-id" orelse 1,
        .host = parsed_args.args.host orelse "127.0.0.1",
        .max_connections = parsed_args.args.@"max-connections" orelse 1,
        .min_connections = parsed_args.args.@"min-connections" orelse 1,
        .port = parsed_args.args.port orelse 8000,
        .token = parsed_args.args.token orelse "",
        .topic_name = parsed_args.positionals[0].?,
    };

    try publish(args);
}

const AdvertiseArgs = struct {
    body: []const u8,
    client_id: u11,
    host: []const u8,
    max_connections: u16,
    min_connections: u16,
    port: u16,
    token: []const u8,
    topic_name: []const u8,
};

fn publish(args: AdvertiseArgs) !void {
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
    defer client.drain();

    std.debug.print("established connection took {}ms\n", .{(connect_end - connect_start) / std.time.ns_per_ms});

    const callback_1 = struct {
        pub fn callback(request: *Message, reply: *Message) void {
            std.debug.print("received request!\n", .{});
            _ = request;
            _ = reply;
        }
    }.callback;

    const callback_1_id = try client.advertise(args.topic_name, callback_1, .{});
    defer client.unadvertise(args.topic_name, callback_1_id, .{ .timeout_ms = 5_000 }) catch unreachable;
}
