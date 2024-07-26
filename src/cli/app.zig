const std = @import("std");
const cli = @import("zig-cli");

const Connection = @import("../proto/connection.zig").Connection;
const Mailbox = @import("../proto/mailbox.zig").Mailbox;
const Message = @import("../proto/message.zig").Message;
const Node = @import("../proto/node.zig").Node;

var node_config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
}{};

var connect_config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
}{};

pub fn run() !void {
    var app_runner = try cli.AppRunner.init(std.heap.page_allocator);

    const version_root_command = cli.Command{
        .name = "version",
        .description = cli.Description{ .one_line = "print versions" },
        .target = .{ .action = .{ .exec = runVersion } },
    };

    const node_run_command = cli.Command{
        .name = "run",
        .description = cli.Description{
            .one_line = "run a node",
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
                .value_ref = app_runner.mkRef(&connect_config.host),
            },
            .{
                .long_name = "port",
                .help = "port to bind to",
                .value_ref = app_runner.mkRef(&connect_config.port),
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
            .target = .{ .subcommands = &.{ node_root_command, version_root_command } },
        },
        .author = "butterworks",
        .version = "cli version 0.0.0",
    };

    return app_runner.run(&app);
}

pub fn runNode() !void {
    std.debug.print("node config.host {s}\n", .{node_config.host});
    std.debug.print("node config.port {d}\n", .{node_config.port});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    var node = try Node.init(allocator, .{
        .host = node_config.host,
        .port = node_config.port,
    });
    defer node.deinit();

    try node.listen();
}

pub fn runPing() !void {
    var mailbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const mailbox_allocator = mailbox_gpa.allocator();
    defer _ = mailbox_gpa.deinit();

    // create inbox & outbox
    var inbox = Mailbox.init(mailbox_allocator);
    defer inbox.deinit();

    var outbox = Mailbox.init(mailbox_allocator);
    defer outbox.deinit();

    // connect to the remote machine
    const addr = try std.net.Address.parseIp(connect_config.host, connect_config.port);
    const stream = try std.net.tcpConnectToAddress(addr);

    var connection = Connection.new(.{ .stream = stream, .inbox = &inbox, .outbox = &outbox });
    defer connection.close();

    // spawn the read and write loops to send and receive data
    connection.run();

    const request = Message.new("node/ping", "");
    try connection.send(request);

    const reply = try connection.receive(1_000);

    std.debug.print("got reply {any}\n", .{reply});
}

pub fn runVersion() !void {
    std.debug.print("0.0.0\n", .{});
}
