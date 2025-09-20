const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;

pub fn ConnectCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                Display this help and exit.
        \\-H, --host    <host>      Node host (default 127.0.0.1)
        \\-p, --port    <port>      Node port (default 8000)
        \\-i, --id      <id>        id of the client (default: 0) 
        \\-t, --token    <token>     authentication token (default: ""),
    );

    const listen_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .id = clap.parsers.int(u11, 10),
        .token = clap.parsers.string,
    };

    // Here we pass the partially parsed argument iterator.
    var diag = clap.Diagnostic{};
    var parsed_args = clap.parseEx(clap.Help, &params, listen_parsers, iter, .{
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

    std.debug.print("args {any}\n", .{parsed_args.args});

    const host = parsed_args.args.host orelse "127.0.0.1";
    const port = parsed_args.args.port orelse 8000;
    const id = parsed_args.args.id orelse 0;
    const token = parsed_args.args.token orelse "";

    try connect(host, port, id, token);
}

pub fn connect(host: []const u8, port: u16, id: u11, token: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const client_config = ClientConfig{
        .authentication_config = .{
            .token_config = .{
                .id = id,
                .token = token,
            },
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = host,
        .port = port,
        .transport = .tcp,
        .reconnect_config = .{
            .enabled = false,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
        .keep_alive_config = .{
            .enabled = true,
            .interval_ms = 300,
        },
    };

    const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
    defer client.disconnect(conn);

    signal_handler.registerSigintHandler();

    while (!signal_handler.sigint_triggered) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}
