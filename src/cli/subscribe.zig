const std = @import("std");
const clap = @import("clap");

pub fn SubscribeCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                              Display this help and exit.
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

    const args = SubscribeArgs{
        .client_id = parsed_args.args.@"client-id" orelse 1,
        .host = parsed_args.args.host orelse "127.0.0.1",
        .max_connections = parsed_args.args.@"max-connections" orelse 1,
        .min_connections = parsed_args.args.@"min-connections" orelse 1,
        .port = parsed_args.args.port orelse 8000,
        .token = parsed_args.args.token orelse "",
        .topic_name = parsed_args.positionals[0].?,
    };

    std.debug.print("Subscribing... \n Host: {s} \n Port: {} \n Client ID: {} \n Token: {s} \n Max Connections: {} \n Min Connections: {} \n Topic Name: {s}", .{ args.host, args.port, args.client_id, args.token, args.max_connections, args.min_connections, args.topic_name });
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
