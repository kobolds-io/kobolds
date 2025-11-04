const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const Broker = @import("../broker/broker.zig").Broker;
const PeerConfig = Broker.Config;
const ListenerConfig = @import("../broker/listener.zig").ListenerConfig;
const AllowedInboundConnectionConfig = @import("../broker/listener.zig").AllowedInboundConnectionConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;

pub fn ListenCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                              Display this help and exit.
        \\-H, --host <host>                       Host to bind to (default 127.0.0.1)
        \\-p, --port <port>                       Port to bind to (default 8000)
        \\-w, --worker-threads <worker_threads>   Number of worker threads to spawn (default 3)
        \\
    );

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

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    const host = parsed_args.args.host orelse "127.0.0.1";
    const port = parsed_args.args.port orelse 8000;
    const worker_threads = parsed_args.args.@"worker-threads" orelse 3;

    try listen(host, port, worker_threads);
}

fn listen(host: []const u8, port: u16, worker_threads: usize) !void {
    // creating a client to communicate with the peer
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var peer_config = PeerConfig{
        .worker_threads = worker_threads,
        .broker_id = 1,
        .authenticator_config = .{
            .token = .{
                .clients = &.{
                    .{ .id = 10, .token = "asdf" }, // for now these are hardcoded
                    .{ .id = 11, .token = "asdf" },
                },
                .peers = &.{},
            },
        },
    };

    // This is just a test used to whitelist a certain inbound connection origins
    const allowed_inbound_connection_config = AllowedInboundConnectionConfig{ .host = "0.0.0.0" };
    const allowed_inbound_connection_configs = [_]AllowedInboundConnectionConfig{allowed_inbound_connection_config};

    const listener_config = ListenerConfig{
        .host = host,
        .port = port,
        .transport = .tcp,
        .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
    };
    const listener_configs = [_]ListenerConfig{listener_config};
    peer_config.listener_configs = &listener_configs;

    // const outbound_peer_connection_config = OutboundConnectionConfig{
    //     .host = "127.0.0.1",
    //     .port = 8006,
    //     .transport = .tcp,
    //     .reconnect_config = .{
    //         .enabled = true,
    //         .max_attempts = 0,
    //         .reconnection_strategy = .timed,
    //     },
    // };

    // const outbound_connection_configs = [_]OutboundConnectionConfig{outbound_peer_connection_config};
    // peer_config.outbound_configs = &outbound_connection_configs;

    var peer = try Broker.init(allocator, peer_config);
    defer peer.deinit();

    try peer.start();
    defer peer.close();

    signal_handler.registerSigintHandler();

    while (!signal_handler.sigint_triggered) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}
