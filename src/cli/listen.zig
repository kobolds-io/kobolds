const std = @import("std");
const log = std.log.scoped(.cli_listen);
const gnoll = @import("gnoll");
const Gnoll = gnoll.Gnoll;
const ConfigInfo = gnoll.ConfigInfo;
const GnollOptions = gnoll.GnollOptions;
const clap = @import("clap");

pub fn ListenCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                              Display this help and exit.
        \\-H, --host <host>                       Host to bind to (default 127.0.0.1)
        \\-p, --port <port>                       Port to bind to (default 8000)
        \\-w, --worker-threads <worker_threads>   Number of worker threads to spawn (default 3)
    );

    const gnoll_options = GnollOptions{
        .config_infos = &.{
            ConfigInfo{
                .filepath = "src/config/listen_config.yaml",
                .format = .yaml,
            },
        },
    };

    const listen_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .worker_threads = clap.parsers.int(usize, 10),
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

    var listenConfig = try Gnoll(ListenConfig).init(allocator, gnoll_options);
    defer listenConfig.deinit(allocator);

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    const args = ListenArgs{
        .host = parsed_args.args.host orelse listenConfig.config.host orelse "127.0.0.1",
        .port = parsed_args.args.port orelse listenConfig.config.port orelse 8000,
        .worker_threads = parsed_args.args.@"worker-threads" orelse listenConfig.config.worker_threads orelse 3,
    };
    log.debug("Listening...  Port: {s} Host: {} Worker Threads: {}", .{ args.host, args.port, args.worker_threads });
}

const ListenConfig = struct { host: ?[]const u8, port: ?u16, worker_threads: ?usize };

const ListenArgs = struct {
    host: []const u8,
    port: u16,
    worker_threads: usize,
};
