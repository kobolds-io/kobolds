const std = @import("std");
const testing = std.testing;

pub const Authenticator = union(AuthenticationStrategyType) {
    const Self = @This();

    none: NoneAuthStrategy,

    pub fn authenticate(self: *Self, context: *anyopaque) bool {
        return switch (self.*) {
            .none => |*s| {
                const c: *NoneAuthStrategy.Context = @ptrCast(@alignCast(context));
                return s.authenticate(c.*);
            },
        };
    }

    pub fn deinit(self: *Self) void {
        return switch (self.*) {
            .none => |*s| return s.deinit(),
        };
    }
};

const AuthenticationStrategyType = enum {
    none,
};

const NoneAuthStrategy = struct {
    const Self = @This();

    pub const Context = struct {};

    pub fn authenticate(_: *Self, _: Context) bool {
        return true;
    }

    pub fn deinit(_: *Self) void {}
};

test "init/deinit" {
    const allocator = testing.allocator;
    _ = allocator;

    // This is just a bullshit test to ensure that we always are able to init/deinit the authenticator
    var authenticator = Authenticator{ .none = .{} };
    defer authenticator.deinit();
}

test "none strategy" {
    const allocator = testing.allocator;
    _ = allocator;

    var authenticator = Authenticator{ .none = .{} };
    defer authenticator.deinit();
    var context = NoneAuthStrategy.Context{};

    try testing.expectEqual(true, authenticator.authenticate(&context));
}
