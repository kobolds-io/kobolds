const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const constants = @import("../constants.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;

pub fn PublishCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                             Display this help and exit.
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

    if (parsed_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &params, .{});
    }

    if (parsed_args.positionals.len != 2) return error.MissingParams;

    const args = PublishArgs{
        .body = parsed_args.positionals[1].?,
        .client_id = parsed_args.args.@"client-id" orelse 1,
        .count = parsed_args.args.count orelse 0,
        .host = parsed_args.args.host orelse "127.0.0.1",
        .max_connections = parsed_args.args.@"max-connections" orelse 1,
        .min_connections = parsed_args.args.@"min-connections" orelse 1,
        .port = parsed_args.args.port orelse 8000,
        .rate = parsed_args.args.rate orelse 0,
        .token = parsed_args.args.token orelse "",
        .topic_name = parsed_args.positionals[0].?,
    };

    // validate
    if (args.rate > 0 and args.count > 0) return error.RateAndCountAreMutuallyExclusive;
    if (args.rate > 1_000_000) return error.RateBreachesArbitraryLimit;
    if (args.count > 100_000_000) return error.CountBreachesArbitraryLimit;

    try publish(args);
}

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

fn publish(args: PublishArgs) !void {
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

    std.debug.print("established connection took {}ms\n", .{(connect_end - connect_start) / std.time.ns_per_ms});

    // defer client.drain(5_000 * std.time.ns_per_ms);

    // var buf: [32]u8 = undefined;
    if (args.count > 0) {
        signal_handler.registerSigintHandler();

        var publish_count_timer = try std.time.Timer.start();
        var last_report = publish_count_timer.read();

        var published: usize = 0;
        while (published < args.count) : (published += 1) {
            if (signal_handler.sigint_triggered) return;

            // const topic_name = "b" ** constants.message_max_topic_name_size;
            // const body = "a" ** constants.message_max_body_size;
            // client.publish(topic_name, body, .{}) catch {
            // client.publish(args.topic_name, body, .{}) catch {
            // const ts = std.time.nanoTimestamp();
            // const str = try std.fmt.bufPrint(&buf, "{d}", .{ts});
            // client.publish(args.topic_name, str, .{}) catch {
            client.publish(args.topic_name, args.body, .{}) catch {
                published -= 1;
                continue;
            };

            const now = publish_count_timer.read();
            if (now - last_report >= std.time.ns_per_s) {
                const remaining = args.count - published - 1;
                std.debug.print("elapsed: {d}ms, published {d}, remaining {d}\n", .{
                    publish_count_timer.read() / std.time.ns_per_ms,
                    published + 1,
                    remaining,
                });
                last_report = now;
            }
        }

        const elapsed = publish_count_timer.read();
        std.debug.print("took {d}ms to publish {d} messages\n", .{
            elapsed / std.time.ns_per_ms,
            args.count,
        });

        // this just keeps the connections open for a bit longer
        std.Thread.sleep(1_000 * std.time.ns_per_ms);
        return;
    }

    if (args.rate == 0) {
        // const ts = std.time.nanoTimestamp();
        // const str = try std.fmt.bufPrint(&buf, "{d}", .{ts});
        // try client.publish(args.topic_name, str, .{});
        try client.publish(args.topic_name, args.body, .{});
    } else {
        signal_handler.registerSigintHandler();

        const period_ns = std.time.ns_per_s / args.rate;

        var next_deadline = std.time.nanoTimestamp();
        var total_messages_sent: u128 = 0;
        var publish_rate_timer = try std.time.Timer.start();
        var last_report = publish_rate_timer.read();

        while (true) {
            if (signal_handler.sigint_triggered) return;

            // schedule next slot
            next_deadline += period_ns;

            // const topic_name = "b" ** constants.message_max_topic_name_size;
            // const body = "a" ** constants.message_max_body_size;
            // client.publish(topic_name, body, .{}) catch {
            // client.publish(args.topic_name, body, .{}) catch {
            // client.publish(args.topic_name, "", .{}) catch {
            // const ts = std.time.nanoTimestamp();
            // const str = try std.fmt.bufPrint(&buf, "{d}", .{ts});
            // client.publish(args.topic_name, str, .{}) catch {
            client.publish(args.topic_name, args.body, .{}) catch {
                // std.debug.print("too many publishes\n", .{});
                std.Thread.sleep(1 * std.time.ns_per_ms);
                continue;
            };

            total_messages_sent += 1;

            // print once per second
            const elapsed = publish_rate_timer.read();
            if (elapsed - last_report >= std.time.ns_per_s) {
                std.debug.print("elapsed: {d}s, total_published: {d}\n", .{
                    elapsed / std.time.ns_per_s,
                    total_messages_sent,
                });
                last_report = elapsed;
            }

            // // pacing
            // const now = std.time.nanoTimestamp();
            // if (next_deadline > now) {
            //     std.Thread.sleep(@intCast(next_deadline - now));
            // } else {
            //     // we fell behind — skip sleeping
            //     next_deadline = now;
            // }
        }
    }
}
