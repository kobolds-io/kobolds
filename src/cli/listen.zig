const std = @import("std");
const clap = @import("clap");
const Node = @import("../node/node.zig").Node;
const NodeConfig = @import("../node/node.zig").NodeConfig;
const ListenerConfig = @import("../node/listener.zig").ListenerConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;
const AllowedInboundConnectionConfig = @import("../node/listener.zig").AllowedInboundConnectionConfig;

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

    std.debug.print("args {any}\n", .{parsed_args.args});

    const host = parsed_args.args.host orelse "127.0.0.1";
    const port = parsed_args.args.port orelse 8000;
    const worker_threads = parsed_args.args.@"worker-threads" orelse 3;

    // std.debug.print("host = {s}\n", .{host});
    // std.debug.print("port = {}\n", .{port});
    // std.debug.print("worker-threads = {}\n", .{worker_threads});

    try listen(host, port, worker_threads);
}

pub fn listen(host: []const u8, port: u16, worker_threads: usize) !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var node_config = NodeConfig{
        .worker_threads = worker_threads,
    };

    // This is just a test used to whitelist a certain inbound connection origins
    const allowed_inbound_connection_config = AllowedInboundConnectionConfig{ .host = "0.0.0.0" };
    const allowed_inbound_connection_configs = [_]AllowedInboundConnectionConfig{allowed_inbound_connection_config};

    // const client_listener_config = ListenerConfig{
    //     .host = "127.0.0.1",
    //     .port = 8000,
    //     .transport = .tcp,
    //     .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
    //     .peer_type = .client,
    // };

    // const node_listener_config = ListenerConfig{
    //     .host = "127.0.0.1",
    //     .port = 8001,
    //     .transport = .tcp,
    //     .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
    //     .peer_type = .node,
    // };

    const listener_config = ListenerConfig{
        .host = host,
        .port = port,
        .transport = .tcp,
        .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
        .peer_type = .client,
    };
    const listener_configs = [_]ListenerConfig{listener_config};
    node_config.listener_configs = &listener_configs;

    const outbound_node_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8006,
        .transport = .tcp,
        .peer_type = .node,
        .reconnect_config = .{
            .enabled = true,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
    };

    const outbound_connection_configs = [_]OutboundConnectionConfig{outbound_node_connection_config};
    node_config.outbound_configs = &outbound_connection_configs;

    var node = try Node.init(allocator, node_config);
    defer node.deinit();

    try node.start();
    defer node.close();

    // registerSigintHandler();

    // while (!sigint_received) {
    //     std.Thread.sleep(1 * std.time.ns_per_ms);
    // }
}
