const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const cli = @import("zig-cli");
const log = std.log.scoped(.CLI);

const constants = @import("../constants.zig");
const Mailbox = @import("../data_structures/mailbox.zig").Mailbox;
const Message = @import("../protocol/message.zig").Message;
const Channel = @import("../data_structures/channel.zig").Channel;
const Topic = @import("../pubsub/topic.zig").Topic;
const Subscriber = @import("../pubsub/subscriber.zig").Subscriber;

const IO = @import("../io.zig").IO;

const Client = @import("../client/client.zig").Client;
const ClientConfig = @import("../client/client.zig").ClientConfig;

const Node = @import("../node/node2.zig").Node;
const NodeConfig = @import("../node/node2.zig").NodeConfig;
const ConnectionConfig = @import("../node/listener.zig").ConnectionConfig;
const ListenerConfig = @import("../node/listener.zig").ListenerConfig;

var client_config = ClientConfig{
    .host = "127.0.0.1",
    .port = 8000,
    .compression = .none,
    .message_pool_capacity = 5_000,
    .max_connections = 10,
};

var node_config2 = NodeConfig{
    .max_connections = 5,
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
                .value_ref = app_runner.mkRef(&node_config2.worker_threads),
            },
        },

        .target = .{ .action = .{ .exec = nodeListen } },
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

    // const node_subscribe_command = cli.Command{
    //     .name = "subscribe",
    //     .description = cli.Description{ .one_line = "subscribe to a topic" },
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
    //     },
    //     .target = .{ .action = .{ .exec = nodeSubscribe } },
    // };

    // const node_publish_command = cli.Command{
    //     .name = "publish",
    //     .description = cli.Description{ .one_line = "publish to a topic" },
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
    //     },
    //     .target = .{ .action = .{ .exec = nodePublish } },
    // };

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
                node_ping_command,
                // node_request_command,
                // node_bench_command,
                // node_reply_command,
                // node_publish_command,
                // node_subscribe_command,
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

    const listener_config = ListenerConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
    };

    const listener_configs = [_]ListenerConfig{listener_config};

    node_config2.listener_configs = &listener_configs;

    var node = try Node.init(allocator, node_config2);
    defer node.deinit();

    try node.start();
    defer node.close();

    registerSigintHandler();

    while (!sigint_received) {}
}

pub fn nodePing() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // remote node to connect to
    const outbound_config = ConnectionConfig{
        .host = "127.0.0.1",
        .port = 8000,
        .transport = .tcp,
    };

    const outbound_configs = [_]ConnectionConfig{outbound_config};

    node_config2.worker_threads = 1;
    node_config2.outbound_configs = &outbound_configs;

    var node = try Node.init(allocator, node_config2);
    defer node.deinit();

    try node.start();
    defer node.close();

    registerSigintHandler();

    while (!sigint_received) {}
}

pub fn nodeReply() !void {
    // log.debug("reply config {any}", .{reply_config});
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();
    //
    // var client = try Client.init(allocator, client_config);
    // defer client.deinit();
    //
    // // spin up the client worker thread
    // try client.run();
    // defer client.close();
    //
    // const connect_deadline_ms: i64 = 1000;
    // const conn = try client.connect(connect_deadline_ms);
    // _ = conn; // NOTE: i'm actually not to sure if I want to give users access to the connection
    //
    // const handler_fn = struct {
    //     fn handle(request: *Message, reply: *Message) void {
    //         log.debug("handling request!", .{});
    //         // echo this back
    //         reply.setBody(request.body());
    //     }
    // }.handle;
    //
    // try client.advertise("/my/topic", handler_fn);
    // defer client.unadvertise("/my/topic") catch unreachable;
    //
    // try registerSigintHandler();
    // while (!sigint_received) {
    //     std.time.sleep(1 * std.time.ns_per_ms);
    // }
}

pub fn nodeRequest() !void {}

pub fn nodeBench() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    // run the background client thread
    try client.start();
    defer client.stop();

    for (0..client.config.max_connections) |_| {
        const conn = try client.connect();
        errdefer client.disconnect(conn);
    }

    defer {
        for (0..client.config.max_connections) |_| {
            var conn_iter = client.connections.valueIterator();
            while (conn_iter.next()) |conn_ptr| {
                const conn = conn_ptr.*;
                errdefer client.disconnect(conn);
            }
        }
    }

    const body = "";
    // const body = "a" ** constants.message_max_body_size;
    const topic_name = "/test";

    while (true) {
        var conn_iter = client.connections.valueIterator();
        while (conn_iter.next()) |conn_ptr| {
            const conn = conn_ptr.*;
            client.publish(conn, topic_name, body, .{}) catch |err| switch (err) {
                error.OutOfMemory => {
                    std.time.sleep(1 * std.time.ns_per_ms);
                    continue;
                },
                else => {
                    std.time.sleep(1 * std.time.ns_per_ms);
                    continue;

                    // @panic("unhandled publish error");
                },
            };
        }
    }
}

pub fn nodePublish() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    // run the background client thread
    try client.start();
    defer client.stop();

    const conn = try client.connect();
    defer client.disconnect(conn);

    // const body = "hello from the publisher";
    const body = "a" ** constants.message_max_body_size;
    const topic_name = "/test";

    registerSigintHandler();

    while (!sigint_received) {
        client.publish(conn, topic_name, body, .{}) catch |err| {
            log.err("error {any}", .{err});
            std.time.sleep(1 * std.time.ns_per_ms);
            continue;
        };
    }
}

pub fn nodeSubscribe() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var client = try Client.init(allocator, client_config);
    defer client.deinit();

    // run the background client thread
    try client.start();
    defer client.stop();

    const conn = try client.connect();
    defer client.disconnect(conn);

    const topic_name = "/test";

    const callback = struct {
        pub fn callback(event: Topic.TopicEvent, context: ?*anyopaque, message: *Message) void {
            _ = event;
            // _ = context;
            const subscriber: *Subscriber = @ptrCast(@alignCast(context));
            subscriber.messages_received += 1;
            if (subscriber.messages_received % 10 == 0) {
                log.debug("subscriber.messages_received {}", .{subscriber.messages_received});
            }
            message.deref();
        }
    }.callback;

    registerSigintHandler();

    const subscriber = try client.subscribe(conn, topic_name, callback, .{});
    defer client.unsubscribe(subscriber) catch unreachable;

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
            log.warn("sigint received", .{});
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
