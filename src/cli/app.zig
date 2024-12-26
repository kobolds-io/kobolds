const std = @import("std");
const assert = std.debug.assert;
const cli = @import("zig-cli");

const constants = @import("../constants.zig");
const Mailbox = @import("../data_structures/mailbox.zig").Mailbox;
const Message = @import("../message.zig").Message;

const Node = @import("../node.zig").Node;
const NodeConfig = @import("../node.zig").NodeConfig;

const Node2 = @import("../node/node.zig").Node;
const NodeConfig2 = @import("../node/node.zig").NodeConfig;

const MessagePool = @import("../message_pool.zig").MessagePool;
const IO = @import("../io.zig").IO;

const Client = @import("../client.zig").Client;
const ClientConfig2 = @import("../client.zig").ClientConfig;

var node_config = NodeConfig{
    .host = "127.0.0.1",
    .port = 8000,
};

var node_config2 = NodeConfig2{
    .host = "127.0.0.1",
    .port = 8000,
    .worker_thread_count = 10,
};

var client_config = ClientConfig2{
    .host = "127.0.0.1",
    .port = 8000,
    .compression = .none,
};

pub fn run() !void {
    var app_runner = try cli.AppRunner.init(std.heap.page_allocator);

    const version_root_command = cli.Command{
        .name = "version",
        .description = cli.Description{ .one_line = "print versions" },
        .target = .{ .action = .{ .exec = runVersion } },
    };

    const node_listen_command = cli.Command{
        .name = "listen",
        .description = cli.Description{
            .one_line = "listen for new connections",
            .detailed = "start a node and listen for incomming connections",
        },

        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&node_config.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&node_config.port),
            },
        },

        .target = .{ .action = .{ .exec = nodeListen } },
    };

    const node_listen2_command = cli.Command{
        .name = "listen2",
        .description = cli.Description{
            .one_line = "listen for new connections",
            .detailed = "start a node and listen for incomming connections",
        },

        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&node_config2.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&node_config2.port),
            },
            .{
                .long_name = "workers",
                .help = "worker threads to be spawned",
                .value_ref = app_runner.mkRef(&node_config2.worker_thread_count),
            },
        },

        .target = .{ .action = .{ .exec = nodeListen2 } },
    };

    const node_ping_command = cli.Command{
        .name = "ping",
        .description = cli.Description{ .one_line = "ping a node" },
        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&client_config.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&client_config.port),
            },
        },
        .target = .{ .action = .{ .exec = runPing } },
    };

    const node_bench_command = cli.Command{
        .name = "bench",
        .description = cli.Description{ .one_line = "benchmark tx/rx to and from a node" },
        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&client_config.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&client_config.port),
            },
        },
        .target = .{ .action = .{ .exec = runBench } },
    };

    const node_request_command = cli.Command{
        .name = "request",
        .description = cli.Description{ .one_line = "send a request to a node" },
        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&client_config.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&client_config.port),
            },
            // TODO: Should capture input and add it as the body of the request
        },
        .target = .{ .action = .{ .exec = runRequest } },
    };

    const node_root_command = cli.Command{
        .name = "node",
        .description = cli.Description{ .one_line = "commands to control nodes" },
        .target = .{
            .subcommands = &.{
                node_listen_command,
                node_listen2_command,
                node_ping_command,
                node_request_command,
                node_bench_command,
            },
        },
    };

    const app = cli.App{
        .command = cli.Command{
            .name = "harpy",
            .description = .{
                .one_line = "a pretty quick messaging system",
                .detailed = "Harpy is a node based messaging system for communicating reliably between machines. Visit [URL] for more information ",
            },
            //            .target = .{ .action = .{ .exec = runNoop } },

            .target = .{ .subcommands = &.{ node_root_command, version_root_command } },
        },
        .author = "butterworks",
        .version = "cli version 0.0.0",
    };

    return app_runner.run(&app);
}

pub fn nodeListen() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var node = try Node.init(allocator, node_config);
    defer node.deinit();

    try node.run();
}

pub fn nodeListen2() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // validate the config
    const max_cpu_count = try std.Thread.getCpuCount();
    assert(node_config2.worker_thread_count >= 1);
    assert(node_config2.worker_thread_count <= max_cpu_count);

    var node = try Node2.init(allocator, node_config2);
    defer node.deinit();

    try node.run();
}

pub fn runPing() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var io = try IO.init(8, 0);
    defer io.deinit();

    var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = message_pool_gpa.deinit();
    const message_pool_allocator = message_pool_gpa.allocator();

    var message_pool = try MessagePool.init(message_pool_allocator, 10_000);
    defer message_pool.deinit();

    var client = try Client.init(allocator, &io, &message_pool, client_config);
    defer client.deinit();

    // run the client which will spawn a thread that will tick in the background
    try client.run();
    defer client.close();

    while (!client.connected or !client.accepted) {
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    try client.ping();

    std.time.sleep(100 * std.time.ns_per_ms);
}

pub fn runRequest() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var io = try IO.init(constants.io_uring_entries, 0);
    defer io.deinit();

    var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = message_pool_gpa.deinit();
    const message_pool_allocator = message_pool_gpa.allocator();

    var message_pool = try MessagePool.init(message_pool_allocator, 10_000);
    defer message_pool.deinit();

    var client = try Client.init(allocator, &io, &message_pool, client_config);
    defer client.deinit();

    // run the client which will spawn a thread that will tick in the background
    try client.run();
    defer client.close();

    while (!client.connected or !client.accepted) {
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    try client.request("/test/topic/", "hello world!");
    std.time.sleep(100 * std.time.ns_per_ms);

    // var try_again = true;
    // while (try_again) {
    //     client.request("/test/topic/", "hello world!") catch |err| switch (err) {
    //         error.ConnectionClosed => {
    //             std.time.sleep(100 * std.time.ns_per_ms);
    //             continue;
    //         },
    //         // error.OutOfMemory => {
    //         //     std.time.sleep(1000 * std.time.ns_per_ms);
    //         //     std.debug.print("slept err {any}\n", .{err});
    //         // },
    //         else => {
    //             std.time.sleep(10 * std.time.ns_per_ms);
    //             std.debug.print("slept err {any}\n", .{err});
    //             std.debug.print("app.request err {any}\n", .{err});
    //             continue;
    //             // break;
    //         },
    //     };
    //     try_again = false;
    // }
}

pub fn runBench() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var io = try IO.init(constants.io_uring_entries, 0);
    defer io.deinit();

    var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = message_pool_gpa.deinit();
    const message_pool_allocator = message_pool_gpa.allocator();

    var message_pool = try MessagePool.init(message_pool_allocator, 10_000);
    defer message_pool.deinit();

    var client = try Client.init(allocator, &io, &message_pool, client_config);
    defer client.deinit();

    // run the client which will spawn a thread that will tick in the background
    try client.run();
    defer client.close();

    while (!client.connected or !client.accepted) {
        std.time.sleep(100 * std.time.ns_per_ms);
    }

    while (true) {
        std.time.sleep(100 * std.time.ns_per_ms);
        for (0..100) |_| {
            client.ping() catch |err| switch (err) {
                error.ConnectionClosed => {
                    std.time.sleep(100 * std.time.ns_per_ms);
                    continue;
                },
                error.OutOfMemory => {
                    std.time.sleep(100 * std.time.ns_per_ms);
                    std.debug.print("slept err {any}\n", .{err});
                },
                else => {
                    std.time.sleep(10 * std.time.ns_per_ms);
                    std.debug.print("slept err {any}\n", .{err});
                    std.debug.print("app.request err {any}\n", .{err});
                    continue;
                    // break;
                },
            };
        }
    }
}

pub fn runVersion() !void {
    std.log.debug("0.0.0", .{});
}

pub fn runNoop() !void {}
