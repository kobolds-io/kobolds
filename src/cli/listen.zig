const std = @import("std");
const clap = @import("clap");

pub fn ListenCommand(allocator: std.mem.Allocator, iter: *std.process.ArgIterator) !void {

    // The parameters for the subcommand.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help                              Display this help and exit.
        \\-p, --ping                              Pong - just for testing.  
    );

    const listen_parsers = .{
        .host = clap.parsers.string,
        .port = clap.parsers.int(u16, 10),
        .worker_threads = clap.parsers.int(usize, 10),
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

    std.debug.print("Pong listen", .{});
}
