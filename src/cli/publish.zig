const std = @import("std");
const log = std.log.scoped(.cli_publish);
const gnoll = @import("gnoll");
const clap = @import("clap");
const utils = @import("../lib/utils.zig");
const Gnoll = gnoll.Gnoll;
const ConfigInfo = gnoll.ConfigInfo;
const GnollOptions = gnoll.GnollOptions;

pub fn PublishCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                              Display this help and exit.
        \\-H, --host        <host>               Node host (default 127.0.0.1)
        \\-p, --port        <port>               Node port (default 8000)
        \\-i, --client-id   <client_id>          id of the client (default: 1) 
        \\-t, --token       <token>              Authentication token (default: ""),
        \\--max-connections <max_connections>    Maximum number of connections to open (default: 1)
        \\--min-connections <min_connections>    Minimum number of connections to open (default: 1)
        \\-r, --rate        <rate>               Rate to publish messages (default: 0)
        \\-c, --count       <count>              Publish a set number of messages (default: 0)
        \\<topic_name>                           Topic name
        \\<body>                                 Body of the message
    );

    const gnoll_options = GnollOptions{
        .config_infos = &.{
            ConfigInfo{
                .filepath = "src/config/publish_config.yaml",
                .format = .yaml,
            },
        },
    };

    const publish_parsers = .{
        .body = clap.parsers.string,
        .client_id = clap.parsers.int(u11, 10),
        .count = clap.parsers.int(u32, 10),
        .host = clap.parsers.string,
        .max_connections = clap.parsers.int(u16, 10),
        .min_connections = clap.parsers.int(u16, 10),
        .port = clap.parsers.int(u16, 10),
        .rate = clap.parsers.int(u32, 10),
        .token = clap.parsers.string,
        .topic_name = clap.parsers.string,
    };

    // Here we pass the partially parsed argument iterator.
    var diag = clap.Diagnostic{};
    var parsed_args = clap.parseEx(clap.Help, &params, publish_parsers, iter, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err; // propagate error
    };
    defer parsed_args.deinit();

    var publishConfig = try Gnoll(PublishConfig).init(allocator, gnoll_options);
    defer publishConfig.deinit(allocator);

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    const args = PublishArgs{
        .body = parsed_args.positionals[1].?,
        .client_id = utils.getConfig(u11, &.{ parsed_args.args.@"client-id", publishConfig.config.client_id }, 1),
        .count = utils.getConfig(u32, &.{ parsed_args.args.count, publishConfig.config.count }, 0),
        .host = utils.getConfig([]const u8, &.{ parsed_args.args.host, publishConfig.config.host }, "127.0.0.1"),
        .max_connections = utils.getConfig(u16, &.{ parsed_args.args.@"max-connections", publishConfig.config.max_connections }, 1),
        .min_connections = utils.getConfig(u16, &.{ parsed_args.args.@"min-connections", publishConfig.config.min_connections }, 1),
        .port = utils.getConfig(u16, &.{ parsed_args.args.port, publishConfig.config.port }, 8000),
        .rate = utils.getConfig(u32, &.{ parsed_args.args.rate, publishConfig.config.rate }, 0),
        .token = utils.getConfig([]const u8, &.{ parsed_args.args.token, publishConfig.config.token }, ""),
        .topic_name = parsed_args.positionals[0].?,
    };

    log.debug("Publishing... Body: {s} Client ID: {} Count: {} Host: {s} Max Connections: {} Min Connections: {} Port: {} Rate: {} Token: {s} Topic Name: {s}", .{ args.body, args.client_id, args.count, args.host, args.max_connections, args.min_connections, args.port, args.rate, args.token, args.topic_name });
}

const PublishConfig = struct {
    client_id: ?u11,
    count: ?u32,
    host: ?[]const u8,
    max_connections: ?u16,
    min_connections: ?u16,
    port: ?u16,
    rate: ?u32,
    token: ?[]const u8,
};

const PublishArgs = struct {
    body: []const u8,
    client_id: u11,
    count: u32,
    host: []const u8,
    max_connections: u16,
    min_connections: u16,
    port: u16,
    rate: u32,
    token: []const u8,
    topic_name: []const u8,
};
