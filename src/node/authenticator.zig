const std = @import("std");
const testing = std.testing;

pub const Authenticator = union(AuthenticationStrategyType) {
    const Self = @This();

    none: NoneAuthStrategy,
    token: TokenAuthStrategy,

    pub fn authenticate(self: *Self, context: *anyopaque) bool {
        return switch (self.*) {
            .none => |*strategy| {
                const c: *NoneAuthStrategy.Context = @ptrCast(@alignCast(context));
                return strategy.authenticate(c.*);
            },
            .token => |*strategy| {
                const c: *TokenAuthStrategy.Context = @ptrCast(@alignCast(context));
                return strategy.authenticate(c.*);
            },
        };
    }

    pub fn deinit(self: *Self) void {
        return switch (self.*) {
            .none => |*strategy| return strategy.deinit(),
            .token => |*strategy| return strategy.deinit(),
        };
    }
};

const AuthenticationStrategyType = enum {
    none,
    token,
};

const NoneAuthStrategy = struct {
    const Self = @This();

    pub const Context = struct {};

    pub fn authenticate(_: *Self, _: Context) bool {
        return true;
    }

    pub fn deinit(_: *Self) void {}
};

const TokenAuthStrategy = struct {
    const Self = @This();

    pub const Context = struct {
        token: []const u8,
    };

    allocator: std.mem.Allocator,
    tokens: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .tokens = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn authenticate(self: *Self, context: Context) bool {
        for (self.tokens.items) |token| {
            if (std.mem.eql(u8, token, context.token)) return true;
        }

        return false;
    }

    pub fn deinit(self: *Self) void {
        self.tokens.deinit();
    }
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

test "token strategy" {
    const allocator = testing.allocator;

    var token_auth_strategy = TokenAuthStrategy.init(allocator);

    const expected_token_1 = "asdf";
    const expected_token_2 = "1234567890";

    try token_auth_strategy.tokens.append(expected_token_1);
    try token_auth_strategy.tokens.append(expected_token_2);

    var authenticator = Authenticator{ .token = token_auth_strategy };
    defer authenticator.deinit();
    var context_1 = TokenAuthStrategy.Context{
        .token = expected_token_1,
    };

    try testing.expectEqual(true, authenticator.authenticate(&context_1));

    var context_2 = TokenAuthStrategy.Context{
        .token = expected_token_2,
    };

    try testing.expectEqual(true, authenticator.authenticate(&context_2));

    const bad_token = "some completely random token!";
    var context_3 = TokenAuthStrategy.Context{
        .token = bad_token,
    };
    try testing.expectEqual(false, authenticator.authenticate(&context_3));
}
