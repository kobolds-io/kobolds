const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const cli = @import("cli");
const log = std.log.scoped(.CLI);

const constants = @import("../constants.zig");
const Connection = @import("../protocol/connection2.zig").Connection;
const Message = @import("../protocol/message.zig").Message;

const IO = @import("../io.zig").IO;

const Node = @import("../node/node.zig").Node;
const NodeConfig = @import("../node/node.zig").NodeConfig;

const Client = @import("../client/client.zig").Client;
const AuthenticationConfig = @import("../client/client.zig").AuthenticationConfig;
const ClientConfig = @import("../client/client.zig").ClientConfig;

const ListenerConfig = @import("../node/listener.zig").ListenerConfig;
const OutboundConnectionConfig = @import("../protocol/connection2.zig").OutboundConnectionConfig;
const AllowedInboundConnectionConfig = @import("../node/listener.zig").AllowedInboundConnectionConfig;

const AuthenticatorConfig = @import("../node/authenticator.zig").AuthenticatorConfig;
const TokenEntry = @import("../node/authenticator.zig").TokenAuthStrategy.TokenEntry;
const Signal = @import("stdx").Signal;

const PublishConfig = struct {
    topic_name: []const u8,
    body: []const u8,
};

var publish_config = PublishConfig{
    .topic_name = "",
    .body = "",
};

var node_config = NodeConfig{
    .max_connections = 5,
    .authenticator_config = .{
        .token = .{
            .clients = &.{
                .{ .id = 10, .token = "asdf" },
                .{ .id = 11, .token = "asdf" },
            },
            .nodes = &.{},
        },
    },
};

var client_config = ClientConfig{
    .max_connections = 100,
    .authentication_config = .{
        .token_config = .{
            .id = 10,
            .token = "asdf",
        },
    },
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

pub fn run(allocator: std.mem.Allocator) !void {
    var app_runner = try cli.AppRunner.init(allocator);
    // defer app_runner.deinit();

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
            .one_line = "connect to a node and disconnect",
            .detailed = "connect to a node, this is purely for debugging",
        },

        .options = &.{},

        .target = .{ .action = .{ .exec = nodeConnect } },
    };

    const node_publish_command = cli.Command{
        .name = "publish",
        .description = cli.Description{
            .one_line = "publish messages",
            .detailed = "publish messages to a topic",
        },

        .options = &.{},

        .target = .{
            .action = .{
                .exec = nodePublish,
                .positional_args = .{
                    .required = &.{
                        .{
                            .name = "topic_name",
                            .help = "topic name to publish the message to",
                            .value_ref = app_runner.mkRef(&publish_config.topic_name),
                        },
                        .{
                            .name = "body",
                            .help = "body of the publish message",
                            .value_ref = app_runner.mkRef(&publish_config.body),
                        },
                    },
                },
            },
        },
    };

    const node_root_command = cli.Command{
        .name = "node",
        .description = cli.Description{ .one_line = "commands to control nodes" },
        .target = .{
            .subcommands = &.{
                node_listen_command,
                node_connect_command,
                node_publish_command,
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
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // This is just a test used to whitelist a certain inbound connection origins
    const allowed_inbound_connection_config = AllowedInboundConnectionConfig{ .host = "0.0.0.0" };
    const allowed_inbound_connection_configs = [_]AllowedInboundConnectionConfig{allowed_inbound_connection_config};

    const client_listener_config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
        .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
        .peer_type = .client,
    };

    const node_listener_config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 8001,
        .transport = .tcp,
        .allowed_inbound_connection_configs = &allowed_inbound_connection_configs,
        .peer_type = .node,
    };

    const listener_configs = [_]ListenerConfig{ client_listener_config, node_listener_config };
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

    registerSigintHandler();

    while (!sigint_received) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}

pub fn nodePing() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const outbound_connection_config = OutboundConnectionConfig{
        .host = "127.0.0.1",
        .port = 8001,
        .transport = .tcp,
        .peer_type = .node,
    };

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    try client.start();
    defer client.close();

    var timer = try std.time.Timer.start();

    const total_start = timer.read();
    const connect_start = timer.read();

    const conn = try client.connect(outbound_connection_config, 5_000 * std.time.ns_per_ms);
    defer client.disconnect(conn);

    const connect_end = timer.read();

    var signals = std.array_list.Managed(*Signal(*Message)).init(allocator);
    defer signals.deinit();

    const ITERATIONS: usize = 1;

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

    const send_start = timer.read();
    var i: usize = 0;
    while (i < ITERATIONS) {
        const signal = signals.items[i];
        client.ping(conn, signal, .{}) catch {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };

        i += 1;
    }
    const send_end = timer.read();

    const receive_start = timer.read();
    for (signals.items) |signal| {
        const rep = try signal.tryReceive(5_000 * std.time.ns_per_ms);

        const error_code = rep.errorCode();
        if (error_code != .ok) return error.BadRequest;

        rep.deref();
        if (rep.refs() == 0) client.memory_pool.destroy(rep);
    }
    const receive_end = timer.read();

    const total_end = timer.read();
    const total_time = (total_end - total_start) / std.time.ns_per_ms;
    const connect_time = (connect_end - connect_start) / std.time.ns_per_ms;
    const send_time = (send_end - send_start) / std.time.ns_per_ms;
    const receive_time = (receive_end - receive_start) / std.time.ns_per_ms;
    log.err("total time took: {d}ms, connect_time: {d}ms, send_time: {d}ms, receive_time: {d}ms", .{
        total_time,
        connect_time,
        send_time,
        receive_time,
    });
}

pub fn nodeConnect() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
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
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
    log.warn("sigint received", .{});
}

pub fn nodePublish() !void {
    log.info("publish_config {any}", .{publish_config});

    // creating a client to communicate with the node
    // var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();

    // const outbound_connection_config = OutboundConnectionConfig{
    //     .host = "127.0.0.1",
    //     .port = 8000,
    //     .transport = .tcp,
    //     .reconnect_config = .{
    //         .enabled = true,
    //         .max_attempts = 0,
    //         .reconnection_strategy = .timed,
    //     },
    //     .keep_alive_config = .{
    //         .enabled = true,
    //         .interval_ms = 300,
    //     },
    // };

    // var client = try Client.init(allocator, client_config);
    // defer client.deinit();

    // try client.start();
    // defer client.close();

    // var connections = std.array_list.Managed(*Connection).init(allocator);
    // defer connections.deinit();

    // const CONNECTION_COUNT = 20;

    // for (0..CONNECTION_COUNT) |_| {
    //     const conn = try client.connect(outbound_connection_config, 10_000 * std.time.ns_per_ms);
    //     errdefer client.disconnect(conn);

    //     try connections.append(conn);
    // }
    // defer {
    //     for (connections.items) |conn| {
    //         client.disconnect(conn);
    //     }
    // }

    // const topic_name = "/test";
    // const body = "";
    // const body = "a" ** constants.message_max_body_size;

    registerSigintHandler();

    // while (!sigint_received) {
    //     for (connections.items) |conn| {
    //         client.publish(conn, topic_name, body, .{}) catch |err| {
    //             log.err("error {any}", .{err});
    //             std.Thread.sleep(10 * std.time.ns_per_ms);
    //             continue;
    //         };
    //     }
    //     // std.Thread.sleep(1 * std.time.ns_per_ms);
    // }
}

var subscriber_msg_count: u64 = 0;
var subscriber_bytes_count: u64 = 0;
pub fn nodeSubscribe() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
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
            subscriber_bytes_count += message.size();
            if (subscriber_msg_count % 100 == 0) {
                log.info(
                    "received message topic: {s}, messages_count: {d}, bytes_count: {d}",
                    .{
                        message.topicName(),
                        subscriber_msg_count,
                        subscriber_bytes_count,
                    },
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
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}

pub fn nodeRequest() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
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

    var signals = std.array_list.Managed(*Signal(*Message)).init(allocator);
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

    const topic_name = "/test";
    const body = "hello from requstor!";
    // const body = "a" ** constants.message_max_body_size;

    var i: usize = 0;
    while (i < ITERATIONS) {
        const send_start = timer.read();
        const signal = signals.items[i];

        client.request(conn, signal, topic_name, body, .{}) catch {
            std.Thread.sleep(1 * std.time.ns_per_ms);
            continue;
        };
        const send_end = timer.read();

        const send_time = (send_end - send_start) / std.time.ns_per_ms;
        if (send_time > 1) {
            log.err("send took > 1ms: {d}ms", .{send_time});
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
            log.err("receive took > 1ms: {d}ms", .{receive_time});
        }
    }

    const total_end = timer.read();
    const total_time = (total_end - total_start) / std.time.ns_per_ms;
    log.err("total time took: {d}ms", .{total_time});
}

var advertiser_msg_count: u64 = 0;
var advertiser_bytes_count: u64 = 0;
pub fn nodeAdvertise() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
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
        pub fn callback(req: *Message, rep: *Message) void {
            advertiser_msg_count += 1;
            advertiser_bytes_count += req.size();
            if (advertiser_msg_count % 100 == 0) {
                log.info(
                    "received message service: {s}, messages_count: {d}, bytes_count: {d}",
                    .{
                        req.topicName(),
                        advertiser_msg_count,
                        advertiser_bytes_count,
                    },
                );
            }

            log.info("received request", .{});
            const body = "hello from advertiser!";
            // const body = "a" ** constants.message_max_body_size;
            rep.setBody(body);
        }
    }.callback;

    var advertise_signal = Signal(*Message).new();
    try client.advertise(conn, &advertise_signal, topic_name, callback, .{});

    {
        const advertise_reply = try advertise_signal.tryReceive(5_000 * std.time.ns_per_ms);
        defer {
            advertise_reply.deref();
            if (advertise_reply.refs() == 0) client.memory_pool.destroy(advertise_reply);
        }

        if (advertise_reply.errorCode() != .ok) return error.BadRequest;

        log.debug("successfully advertising {s}", .{topic_name});
    }

    registerSigintHandler();

    while (!sigint_received) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
}

pub fn version() !void {
    log.info("0.1.0", .{});
}

pub fn noop() !void {}

var sigint_received: bool = false;

/// Intercepts the SIGINT signal and allows for actions after the signal is triggered
fn registerSigintHandler() void {
    const onSigint = struct {
        fn onSigint(_: i32) callconv(.c) void {
            sigint_received = true;
        }
    }.onSigint;

    const mask: [1]c_ulong = [_]c_ulong{0};
    var sa = posix.Sigaction{
        .mask = mask,
        .flags = 0,
        .handler = .{ .handler = onSigint },
    };

    posix.sigaction(posix.SIG.INT, &sa, null);
}
