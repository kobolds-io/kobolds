const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const cli = @import("zig-cli");
const log = std.log.scoped(.CLI);

const constants = @import("../constants.zig");
const Mailbox = @import("../data_structures/mailbox.zig").Mailbox;
const Connection = @import("../protocol/connection.zig").Connection;
const Message = @import("../protocol/message.zig").Message;

const IO = @import("../io.zig").IO;

const Node = @import("../node/node.zig").Node;
const NodeConfig = @import("../node/node.zig").NodeConfig;
const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;
const Transaction = @import("../client/client.zig").Transaction;
const OutboundConnectionConfig = @import("../protocol/connection.zig").OutboundConnectionConfig;
const ListenerConfig = @import("../node/listener.zig").ListenerConfig;
const AllowedInboundConnectionConfig = @import("../node/listener.zig").AllowedInboundConnectionConfig;
const Signal = @import("stdx").Signal;

var node_config = NodeConfig{
    .max_connections = 5,
};

var client_config = ClientConfig{
    .max_connections = 100,
};

const RequestConfig = struct {
    topic: []const u8,
    body: []const u8,
};

const ReplyConfig = struct {
    topic: []const u8,
    body: []const u8,
};

var reply_config = ReplyConfig{
    .topic = undefined,
    .body = undefined,
};

var request_config = RequestConfig{
    .topic = undefined,
    .body = undefined,
};

var config = struct {
    host: []const u8 = "localhost",
    port: u16 = undefined,
}{};

pub fn run() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var app_runner = try cli.AppRunner.init(allocator);

    const version_root_command = cli.Command{
        .name = "version",
        .description = cli.Description{ .one_line = "print versions" },
        .target = cli.CommandTarget{ .action = cli.CommandAction{ .exec = version } },
    };

    const node_listen_command = cli.Command{
        .name = "listen",
        .description = cli.Description{
            .one_line = "listen for incomming connections",
            .detailed = "start a node and listen for incomming connections",
        },

        .options = &.{
            // .{
            //     .long_name = "host",
            //     .help = "host to listen on",
            //     .value_ref = app_runner.mkRef(&node_config.host),
            // },
            // .{
            //     .long_name = "port",
            //     .help = "port to bind to",
            //     .value_ref = app_runner.mkRef(&node_config.port),
            // },
            .{
                .long_name = "worker-threads",
                .help = "worker threads to be spawned",
                .value_ref = app_runner.mkRef(&node_config.worker_threads),
            },
        },

        .target = .{ .action = .{ .exec = nodeListen } },
    };

    const node_connect_command = cli.Command{
        .name = "connect",
        .description = cli.Description{
            .one_line = "connect a node to another node",
            .detailed = "connect a node to another node, this is purely for debugging",
        },

        .options = &.{
            // .{
            //     .long_name = "host",
            //     .help = "host to listen on",
            //     .value_ref = app_runner.mkRef(&node_config.host),
            // },
            // .{
            //     .long_name = "port",
            //     .help = "port to bind to",
            //     .value_ref = app_runner.mkRef(&node_config.port),
            // },
            .{
                .long_name = "worker-threads",
                .help = "worker threads to be spawned",
                .value_ref = app_runner.mkRef(&node_config.worker_threads),
            },
        },

        .target = .{ .action = .{ .exec = nodeConnect } },
    };

    const node_ping_command = cli.Command{
        .name = "ping",
        .description = cli.Description{ .one_line = "ping a node" },
        // .options = &.{
        //     .{
        //         .long_name = "host",
        //         .help = "host to listen on",
        //         .value_ref = app_runner.mkRef(&node_config2.host),
        //     },
        //     .{
        //         .long_name = "port",
        //         .help = "port to bind to",
        //         .value_ref = app_runner.mkRef(&node_config2.port),
        //     },
        // },
        .target = .{ .action = .{ .exec = nodePing } },
    };

    const node_subscribe_command = cli.Command{
        .name = "subscribe",
        .description = cli.Description{ .one_line = "subscribe to a topic" },
        .options = &.{
            // .{
            //     .long_name = "host",
            //     .help = "host to listen on",
            //     .value_ref = app_runner.mkRef(&client_config.host),
            // },
            // .{
            //     .long_name = "port",
            //     .help = "port to bind to",
            //     .value_ref = app_runner.mkRef(&client_config.port),
            // },
        },
        .target = .{ .action = .{ .exec = nodeSubscribe } },
    };

    const node_publish_command = cli.Command{
        .name = "publish",
        .description = cli.Description{ .one_line = "publish to a topic" },
        .options = &.{
            // .{
            //     .long_name = "host",
            //     .help = "host to listen on",
            //     .value_ref = app_runner.mkRef(&client_config.host),
            // },
            // .{
            //     .long_name = "port",
            //     .help = "port to bind to",
            //     .value_ref = app_runner.mkRef(&client_config.port),
            // },
        },
        .target = .{ .action = .{ .exec = nodePublish } },
    };

    // const node_bench_command = cli.Command{
    //     .name = "bench",
    //     .description = cli.Description{ .one_line = "benchmark tx/rx to and from a node" },
    //     .options = &.{
    //         .{
    //             .long_name = "host",
    //             .help = "host to listen on",
    //             .value_ref = app_runner.mkRef(&client_config.host),
    //         },
    //         .{
    //             .long_name = "port",
    //             .help = "port to bind to",
    //             .value_ref = app_runner.mkRef(&client_config.port),
    //         },
    //         .{
    //             .long_name = "max-connections",
    //             .help = "maximum number of connections for the client",
    //             .value_ref = app_runner.mkRef(&client_config.max_connections),
    //         },
    //     },

    //     .target = .{ .action = .{ .exec = nodeBench } },
    // };

    // const node_request_command = cli.Command{
    //     .name = "request",
    //     .description = cli.Description{ .one_line = "send a request to a node" },
    //     .options = &.{
    //         .{
    //             .long_name = "host",
    //             .help = "host to listen on",
    //             .value_ref = app_runner.mkRef(&client_config.host),
    //         },
    //         .{
    //             .long_name = "port",
    //             .help = "port to bind to",
    //             .value_ref = app_runner.mkRef(&client_config.port),
    //         },
    //         // TODO: Should capture input and add it as the body of the request
    //     },
    //     .target = .{ .action = .{ .exec = nodeRequest } },
    // };

    // const node_reply_command = cli.Command{
    //     .name = "reply",
    //     .description = cli.Description{ .one_line = "reply to requests sent to a topic" },
    //     .options = &.{
    //         cli.Option{
    //             .long_name = "host",
    //             .help = "host to listen on",
    //             .value_ref = app_runner.mkRef(&client_config.host),
    //         },
    //         cli.Option{
    //             .long_name = "port",
    //             .help = "port to bind to",
    //             .value_ref = app_runner.mkRef(&client_config.port),
    //         },
    //         // TODO: Should capture input and add it as the body of the request
    //     },
    //     .target = cli.CommandTarget{
    //         .action = cli.CommandAction{
    //             .positional_args = cli.PositionalArgs{
    //                 .required = &.{
    //                     .{
    //                         .name = "topic",
    //                         .help = "topic of the reply",
    //                         .value_ref = app_runner.mkRef(&reply_config.topic),
    //                     },
    //                 },
    //                 .optional = &.{
    //                     .{
    //                         .name = "body",
    //                         .help = "body of the reply",
    //                         .value_ref = app_runner.mkRef(&reply_config.body),
    //                     },
    //                 },
    //             },
    //             .exec = nodeReply,
    //         },
    //     },
    // };

    const node_root_command = cli.Command{
        .name = "node",
        .description = cli.Description{ .one_line = "commands to control nodes" },
        .target = .{
            .subcommands = &.{
                node_listen_command,
                node_connect_command,
                node_ping_command,
                // node_request_command,
                // node_bench_command,
                // node_reply_command,
                node_publish_command,
                node_subscribe_command,
            },
        },
    };

    const app = cli.App{
        .command = cli.Command{
            .name = "kobolds",
            .description = cli.Description{
                .one_line = "a quick and reliable messaging system",
                .detailed = "Kobolds is a node based messaging system for communicating reliably between machines. Visit [URL] for more information ",
            },
            .target = cli.CommandTarget{
                .subcommands = &.{
                    node_root_command,
                    version_root_command,
                },
            },
        },
        .author = "kobolds",
        .version = "cli version 0.0.0",
    };

    return app_runner.run(&app);
}

pub fn nodeListen() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // This is just a test used to whitelist a certain inbound connection origins
    const allowed_inbound_connection_config = AllowedInboundConnectionConfig{
        .host = "0.0.0.0",
    };
    const allowed_inbound_connection_configs = [_]AllowedInboundConnectionConfig{allowed_inbound_connection_config};
    const listener_config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
    };
    const listener_configs = [_]ListenerConfig{listener_config};
    node_config.listener_configs = &listener_configs;

    var node = try Node.init(allocator, node_config);
    defer node.deinit();

    try node.start();
    defer node.close();

    registerSigintHandler();

    while (!sigint_received) {
        std.time.sleep(1 * std.time.ns_per_ms);
    }
}

pub fn nodePing() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .reconnect_config = .{
            .enabled = true,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
        .keep_alive_config = .{
            .enabled = true,
            .interval_ms = 300,
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
    defer client.disconnect(conn);

    var signals = std.ArrayList(*Signal(*Message)).init(allocator);
    defer signals.deinit();

    const ITERATIONS: usize = 1;

    var timer = try std.time.Timer.start();
    const total_start = timer.read();
    for (0..ITERATIONS) |_| {
        const signal = try allocator.create(Signal(*Message));
        errdefer allocator.destroy(signal);

        signal.* = Signal(*Message).new();
        try signals.append(signal);
    }
    defer {
        for (signals.items) |signal| {
            allocator.destroy(signal);
        }
    }

    var i: usize = 0;
    while (i < ITERATIONS) {
        const send_start = timer.read();
        const signal = signals.items[i];
        client.ping(conn, signal, .{}) catch {
            std.time.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        const send_end = timer.read();

        const send_time = (send_end - send_start) / std.time.ns_per_ms;
        if (send_time > 1) {
            log.err("send took > 1ms: {}ms", .{send_time});
        }

        i += 1;
    }

    for (signals.items) |signal| {
        const receive_start = timer.read();
        const rep = try signal.tryReceive(5_000 * std.time.ns_per_ms);
        const receive_end = timer.read();

        const error_code = rep.errorCode();
        if (error_code != .ok) return error.BadRequest;

        rep.deref();
        if (rep.refs() == 0) client.memory_pool.destroy(rep);

        const receive_time = (receive_end - receive_start) / std.time.ns_per_ms;
        if (receive_time > 1) {
            log.err("receive took > 1ms: {}ms", .{receive_time});
        }
    }

    const total_end = timer.read();
    const total_time = (total_end - total_start) / std.time.ns_per_ms;
    log.err("total time took: {}ms", .{total_time});
}

pub fn nodeConnect() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .reconnect_config = .{
            .enabled = true,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
        .keep_alive_config = .{
            .enabled = true,
            .interval_ms = 300,
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
    defer client.disconnect(conn);

    registerSigintHandler();

    while (!sigint_received) {
        std.time.sleep(1 * std.time.ns_per_ms);
    }
    log.warn("sigint received", .{});
}

pub fn nodePublish() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .reconnect_config = .{
            .enabled = true,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
        .keep_alive_config = .{
            .enabled = true,
            .interval_ms = 300,
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    var connections = std.ArrayList(*Connection).init(allocator);
    defer connections.deinit();

    const CONNECTION_COUNT = 20;

    for (0..CONNECTION_COUNT) |_| {
        const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
        errdefer client.disconnect(conn);
        // std.time.sleep(100 * std.time.ns_per_ms);

        try connections.append(conn);
    }
    defer {
        for (connections.items) |conn| {
            client.disconnect(conn);
        }
    }

    const topic_name = "/test";
    // const body = "";
    const body = "a" ** constants.message_max_body_size;

    registerSigintHandler();

    while (!sigint_received) {
        for (connections.items) |conn| {
            client.publish(conn, topic_name, body, .{}) catch |err| {
                log.err("error {any}", .{err});
                std.time.sleep(10 * std.time.ns_per_ms);
                continue;
            };
        }
        std.time.sleep(1 * std.time.ns_per_ms);
    }
}

var subscriber_msg_count: usize = 0;
pub fn nodeSubscribe() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .reconnect_config = .{
            .enabled = true,
            .max_attempts = 0,
            .reconnection_strategy = .timed,
        },
        .keep_alive_config = .{
            .enabled = true,
            .interval_ms = 300,
        },
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
    defer client.disconnect(conn);

    const topic_name = "/test";

    const callback = struct {
        pub fn callback(message: *Message) void {
            subscriber_msg_count += 1;
            if (subscriber_msg_count % 5 == 0) {
                log.info(
                    "received message topic: {s}, count: {}",
                    .{ message.topicName(), subscriber_msg_count },
                );
            }
        }
    }.callback;

    var sub_signal = Signal(*Message).new();
    // var unsub_signal = Signal(*Message).new();
    try client.subscribe(conn, &sub_signal, topic_name, callback, .{});
    // defer client.unsubscribe(conn, &unsub_signal, topic_name, callback) catch |err| log.err("could not unsubscribe: {any}", .{err});

    {
        const subscribe_reply = try sub_signal.tryReceive(5_000 * std.time.ns_per_ms);
        defer {
            subscribe_reply.deref();
            if (subscribe_reply.refs() == 0) client.memory_pool.destroy(subscribe_reply);
        }

        if (subscribe_reply.errorCode() != .ok) return error.BadRequest;

        log.debug("successfully subscribed", .{});
    }

    registerSigintHandler();

    while (!sigint_received) {
        std.time.sleep(1 * std.time.ns_per_ms);
    }
}

pub fn version() !void {
    std.log.debug("0.0.0", .{});
}

pub fn noop() !void {}

var sigint_received: bool = false;

/// Intercepts the SIGINT signal and allows for actions after the signal is triggered
fn registerSigintHandler() void {
    const onSigint = struct {
        fn onSigint(_: i32) callconv(.C) void {
            sigint_received = true;
        }
    }.onSigint;

    const mask: [32]u32 = [_]u32{0} ** 32;
    var sa = posix.Sigaction{
        .mask = mask,
        .flags = 0,
        .handler = .{ .handler = onSigint },
    };

    posix.sigaction(posix.SIG.INT, &sa, null);
}
