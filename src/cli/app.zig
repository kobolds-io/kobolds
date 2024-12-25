const std = @import("std");
const assert = std.debug.assert;
const cli = @import("zig-cli");

const constants = @import("../constants.zig");
const Mailbox = @import("../data_structures/mailbox.zig").Mailbox;
const Message = @import("../message.zig").Message;

const Node2 = @import("../node.zig").Node;
const NodeConfig2 = @import("../node.zig").NodeConfig;
const MessageBus = @import("../message_bus.zig").MessageBus;
const MessagePool = @import("../message_pool.zig").MessagePool;
const IO = @import("../io.zig").IO;

const Client2 = @import("../client.zig").Client;
const ClientConfig2 = @import("../client.zig").ClientConfig;

var node_config2 = NodeConfig2{
    .host = "127.0.0.1",
    .port = 8000,
};

var client_config2 = ClientConfig2{
    .host = "127.0.0.1",
    .port = 8000,
    .compression = .None,
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
                .value_ref = app_runner.mkRef(&node_config2.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&node_config2.port),
            },
        },

        .target = .{ .action = .{ .exec = nodeListen } },
    };

    const node_ping_command = cli.Command{
        .name = "ping",
        .description = cli.Description{ .one_line = "ping a node" },
        .options = &.{
            .{
                .long_name = "host",
                .help = "host to listen on",
                .value_ref = app_runner.mkRef(&client_config2.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&client_config2.port),
            },
        },
        .target = .{ .action = .{ .exec = runPing2 } },
    };

    const node_root_command = cli.Command{
        .name = "node",
        .description = cli.Description{ .one_line = "commands to control nodes" },
        .target = .{
            .subcommands = &.{
                node_listen_command,
                node_ping_command,
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

    var io = try IO.init(8, 0);
    defer io.deinit();

    var message_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = message_pool_gpa.deinit();
    const message_pool_allocator = message_pool_gpa.allocator();

    var message_pool = try MessagePool.init(message_pool_allocator, constants.message_pool_max_size);
    defer message_pool.deinit();

    var message_bus_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = message_bus_gpa.deinit();
    const message_bus_allocator = message_bus_gpa.allocator();

    var message_bus = try MessageBus.init(message_bus_allocator, &io, &message_pool);
    defer message_bus.deinit();

    var node = try Node2.init(allocator, node_config2, &io, &message_bus, &message_pool);
    defer node.deinit();

    try node.run();
}

pub fn runPing2() !void {
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

    // var message_bus_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = message_bus_gpa.deinit();
    // const message_bus_allocator = message_bus_gpa.allocator();
    //
    // var message_bus = try MessageBus.init(message_bus_allocator, &io, &message_pool);
    // defer message_bus.deinit();

    var client = try Client2.init(allocator, &io, &message_pool, client_config2);
    defer client.deinit();

    // run the client which will spawn a thread that will tick in the background
    try client.run();
    defer client.close();

    // try client.connect();
    // connect reply is a handle that helps me resolve this request
    // try client.connect();

    while (true) {
        std.time.sleep(100 * std.time.ns_per_ms);
        for (0..100) |_| {
            client.ping() catch |err| switch (err) {
                error.ConnectionClosed => {
                    std.time.sleep(100 * std.time.ns_per_ms);
                },
                // error.OutOfMemory => {
                //     std.time.sleep(1000 * std.time.ns_per_ms);
                //     std.debug.print("slept err {any}\n", .{err});
                // },
                else => {
                    std.time.sleep(1000 * std.time.ns_per_ms);
                    std.debug.print("slept err {any}\n", .{err});
                    std.debug.print("app.ping2 err {any}\n", .{err});
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
