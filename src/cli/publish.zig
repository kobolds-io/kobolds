const std = @import("std");
const clap = @import("clap");
const signal_handler = @import("../signal_handler.zig");

const constants = @import("../constants.zig");

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;

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

    const listen_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .client_id = clap.parsers.int(u11, 10),
        .token = clap.parsers.string,
        .max_connections = clap.parsers.int(u16, 10),
        .min_connections = clap.parsers.int(u16, 10),
        .topic_name = clap.parsers.string,
        .body = clap.parsers.string,
        .rate = clap.parsers.int(u32, 10),
        .count = clap.parsers.int(u32, 10),
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

    if (parsed_args.positionals.len != 2) return error.MissingParams;

    const args = PublishArgs{
        .host = parsed_args.args.host orelse "127.0.0.1",
        .port = parsed_args.args.port orelse 8000,
        .client_id = parsed_args.args.@"client-id" orelse 1,
        .topic_name = parsed_args.positionals[0].?,
        .body = parsed_args.positionals[1].?,
        .token = parsed_args.args.token orelse "",
        .max_connections = parsed_args.args.@"max-connections" orelse 1,
        .min_connections = parsed_args.args.@"min-connections" orelse 1,
        .rate = parsed_args.args.rate orelse 0,
        .count = parsed_args.args.count orelse 0,
    };

    // validate
    if (args.rate > 0 and args.count > 0) return error.RateAndCountAreMutuallyExclusive;
    if (args.rate > 1_000_000) return error.RateBreachesArbitraryLimit;
    if (args.count > 100_000_000) return error.CountBreachesArbitraryLimit;

    try publish(args);
}

const PublishArgs = struct {
    host: []const u8,
    port: u16,
    client_id: u11,
    token: []const u8,
    max_connections: u16,
    min_connections: u16,
    topic_name: []const u8,
    body: []const u8,
    rate: u32,
    count: u32,
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

    std.debug.print("connection took {}ms\n", .{(connect_end - connect_start) / std.time.ns_per_ms});

    // signal_handler.registerSigintHandler();
    // while (!signal_handler.sigint_triggered) {
    //     client.publish(args.topic_name, args.body, .{}) catch {
    //         // const b = "a" ** constants.message_max_body_size;
    //         // client.publish(args.topic_name, b, .{}) catch {
    //         // std.Thread.sleep(1 * std.time.ns_per_ms);
    //     };
    // }

    if (args.count > 0) {
        signal_handler.registerSigintHandler();

        var publish_count_timer = try std.time.Timer.start();
        var last_report = publish_count_timer.read();

        var published: usize = 0;
        while (published < args.count) : (published += 1) {
            if (signal_handler.sigint_triggered) return;

            client.publish(args.topic_name, args.body, .{}) catch {
                published -= 1;
                continue;
                // std.Thread.sleep(1 * std.time.ns_per_ms);
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

        // FIX: ensure that all messages have been sent
        // client.flush();

        const elapsed = publish_count_timer.read();
        std.debug.print("took {d}ms to publish {d} messages\n", .{
            elapsed / std.time.ns_per_ms,
            args.count,
        });

        // this just keeps the connections open for a bit longer
        std.Thread.sleep(500 * std.time.ns_per_ms);
        return;
    }

    if (args.rate == 0) {
        try client.publish(args.topic_name, args.body, .{});
    } else {
        signal_handler.registerSigintHandler();

        const period_ns = std.time.ns_per_s / args.rate;

        var next_deadline = std.time.nanoTimestamp();
        var messages_published_in_period: usize = 0;
        var total_messages_sent: u128 = 0;
        var publish_rate_timer = try std.time.Timer.start();
        var last_report = publish_rate_timer.read();

        while (messages_published_in_period < args.rate) : (messages_published_in_period += 1) {
            if (signal_handler.sigint_triggered) return;
            next_deadline += period_ns;

            client.publish(args.topic_name, args.body, .{}) catch {
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            };

            total_messages_sent += 1;

            if (publish_rate_timer.read() - last_report >= std.time.ns_per_s) {
                std.debug.print("elapsed: {d}s, total_published: {d}\n", .{
                    publish_rate_timer.read() / std.time.ns_per_s,
                    total_messages_sent,
                });
                last_report = publish_rate_timer.read();
            }

            const now = std.time.nanoTimestamp();
            if (next_deadline > now) {
                std.Thread.sleep(@intCast(next_deadline - now));
                // we reset this if we have finished this loop
                messages_published_in_period = 0;
            } else {
                // std.debug.print("we're late skip sleep\n", .{});
            }
        }
    }
}
