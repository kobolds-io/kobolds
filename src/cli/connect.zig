const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;

pub fn ConnectCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                             Display this help and exit.
        \\-H, --host        <host>               Node host (default 127.0.0.1)
        \\-p, --port        <port>               Node port (default 8000)
        \\-i, --client-id   <client_id>          id of the client (default: 1) 
        \\-t, --token       <token>              Authentication token (default: ""),
        \\--max-connections <max_connections>    Maximum number of connections to open (default: 1)
    );

    const listen_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .client_id = clap.parsers.int(u11, 10),
        .token = clap.parsers.string,
        .max_connections = clap.parsers.int(u16, 10),
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

    const host = parsed_args.args.host orelse "127.0.0.1";
    const port = parsed_args.args.port orelse 8000;
    const client_id = parsed_args.args.@"client-id" orelse 1;
    const token = parsed_args.args.token orelse "";
    const max_connections = parsed_args.args.@"max-connections" orelse 1;

    try connect(host, port, client_id, token, max_connections);
}

fn connect(host: []const u8, port: u16, client_id: u11, token: []const u8, max_connections: u16) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const client_config = ClientConfig{
        .client_id = client_id,
        .host = host,
        .port = port,
        .max_connections = max_connections,
        .authentication_config = .{
            .token_config = .{
                .id = client_id,
                .token = token,
            },
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    signal_handler.registerSigintHandler();

    while (!signal_handler.sigint_triggered) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}
