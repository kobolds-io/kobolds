const std = @import("std");
const clap = @import("clap");

const ListenCommand = @import("./listen.zig").ListenCommand;
const ConnectCommand = @import("./connect.zig").ConnectCommand;
const PublishCommand = @import("./publish.zig").PublishCommand;

pub fn RootCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {
    const RootSubCommands = enum {
        help,
        listen,
        connect,
        publish,
    };

    const root_parsers = .{ .subcommand = clap.parsers.enumeration(RootSubCommands) };

    const root_params = comptime clap.parseParamsComptime(
        \\-h, --help         Display this help and exit
        \\<subcommand>
    );

    // const RootArgs = clap.ResultEx(clap.Help, &root_params, &root_parsers);
    // discard the first argument
    _ = iter.next();

    var diag = clap.Diagnostic{};
    var parsed_root_args = clap.parseEx(clap.Help, &root_params, root_parsers, iter, .{
        .diagnostic = &diag,
        .allocator = allocator,

        // Terminate the parsing of arguments after parsing the first positional (0 is passed
        // here because parsed positionals are, like slices and arrays, indexed starting at 0).
        //
        // This will terminate the parsing after parsing the subcommand enum and leave `iter`
        // not fully consumed. It can then be reused to parse the arguments for subcommands.
        .terminating_positional = 0,
    }) catch |err| {
        try diag.reportToFile(.stderr(), err);
        return err;
    };
    defer parsed_root_args.deinit();

    if (parsed_root_args.args.help != 0) {
        return clap.helpToFile(.stderr(), clap.Help, &root_params, .{});
    }

    const command = parsed_root_args.positionals[0] orelse return error.MissingCommand;
    switch (command) {
        .help => return clap.helpToFile(.stderr(), clap.Help, &root_params, .{}),
        .listen => try ListenCommand(allocator, iter),
        .connect => try ConnectCommand(allocator, iter),
        .publish => try PublishCommand(allocator, iter),
    }
}
