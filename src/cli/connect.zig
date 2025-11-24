const std = @import("std");
const clap = @import("clap");
const gnoll = @import("gnoll");
const utils = @import("../lib/utils.zig");
const log = std.log.scoped(.cli_connect);
const Gnoll = gnoll.Gnoll;
const ConfigInfo = gnoll.ConfigInfo;
const GnollOptions = gnoll.GnollOptions;

pub fn ConnectCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {
    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                             Display this help and exit.
        \\-H, --host        <host>               Node host (default 127.0.0.1)
        \\-p, --port        <port>               Node port (default 8000)
        \\-i, --client-id   <client_id>          id of the client (default: 1) 
        \\-t, --token       <token>              Authentication token (default: ""),
        \\--max-connections <max_connections>    Maximum number of connections to open (default: 1)
        \\--min-connections <min_connections>    Minimum number of connections to open (default: 1)
    );

    const gnoll_options = GnollOptions{
        .config_infos = &.{
            ConfigInfo{
                .filepath = "src/config/connect_config.yaml",
                .format = .yaml,
            },
        },
    };

    const connect_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .client_id = clap.parsers.int(u11, 10),
        .token = clap.parsers.string,
        .max_connections = clap.parsers.int(u16, 10),
        .min_connections = clap.parsers.int(u16, 10),
    };

    // Here we pass the partially parsed argument iterator.
    var diag = clap.Diagnostic{};
    var parsed_args = clap.parseEx(clap.Help, &params, connect_parsers, iter, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err; // propagate error
    };
    defer parsed_args.deinit();

    var connectionConfig = try Gnoll(ConnectConfig).init(allocator, gnoll_options);
    defer connectionConfig.deinit(allocator);

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    const args = ConnectArgs{
        .host = utils.getConfig([]const u8, &.{ parsed_args.args.host, connectionConfig.config.host }, "127.0.0.1"),
        .port = utils.getConfig(u16, &.{ parsed_args.args.port, connectionConfig.config.port }, 8000),
        .client_id = utils.getConfig(u11, &.{ parsed_args.args.@"client-id", connectionConfig.config.client_id }, 1),
        .token = utils.getConfig([]const u8, &.{ parsed_args.args.token, connectionConfig.config.token }, ""),
        .max_connections = utils.getConfig(u16, &.{ parsed_args.args.@"max-connections", connectionConfig.config.max_connections }, 1),
        .min_connections = utils.getConfig(u16, &.{ parsed_args.args.@"min-connections", connectionConfig.config.min_connections }, 1),
    };

    log.debug("Connecting... Host: {s} Port: {} Client ID: {} Token: {s} Max Connections: {} Min Connections: {}", .{ args.host, args.port, args.client_id, args.token, args.max_connections, args.min_connections });
}

const ConnectConfig = struct {
    host: ?[]const u8,
    port: ?u16,
    client_id: ?u11,
    token: ?[]const u8,
    max_connections: ?u16,
    min_connections: ?u16,
};

const ConnectArgs = struct {
    host: []const u8,
    port: u16,
    client_id: u11,
    token: []const u8,
    max_connections: u16,
    min_connections: u16,
};
