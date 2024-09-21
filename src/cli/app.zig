const std = @import("std");
const assert = std.debug.assert;
const cli = @import("zig-cli");

const Connection = @import("../protocol/connection.zig").Connection;
const Mailbox = @import("../protocol/mailbox.zig").Mailbox;
const Message = @import("../protocol/message.zig").Message;
const Node = @import("../protocol/node.zig").Node;
const NodeConfig = @import("../protocol/node.zig").NodeConfig;
const Client = @import("../protocol/client.zig").Client;
const ClientConfig = @import("../protocol/client.zig").ClientConfig;

var node_config = NodeConfig{
    .host = "127.0.0.1",
    .port = 4000,
};

var client_config = ClientConfig{
    .host = "127.0.0.1",
    .port = 4000,
};

pub fn run() !void {
    var app_runner = try cli.AppRunner.init(std.heap.page_allocator);

    const version_root_command = cli.Command{
        .name = "version",
        .description = cli.Description{ .one_line = "print versions" },
        .target = .{ .action = .{ .exec = runVersion } },
    };

    const node_run_command = cli.Command{
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

        .target = .{ .action = .{ .exec = runNode } },
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

    const node_root_command = cli.Command{
        .name = "node",
        .description = cli.Description{ .one_line = "commands to control nodes" },
        .target = .{
            .subcommands = &.{ node_run_command, node_ping_command },
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

pub fn runNode() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var node = Node.init(allocator, node_config);
    defer node.deinit();

    try node.run();
}

pub fn runPing() !void {
    // creating a client to communicate with the node
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var client = Client.init(allocator, client_config);
    defer client.deinit();

    try client.connect();
    defer client.close();

    // try client.ping();
    // std.time.sleep(std.time.ns_per_ms * 10);
}

pub fn runVersion() !void {
    std.log.debug("0.0.0", .{});
}

pub fn runNoop() !void {}
