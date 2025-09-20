const std = @import("std");
const posix = std.posix;

pub var sigint_triggered: bool = false;

/// Intercepts the SIGINT signal and allows for actions after the signal is triggered
pub fn registerSigintHandler() void {
    const onSigint = struct {
        fn onSigint(_: i32) callconv(.c) void {
            sigint_triggered = true;
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
