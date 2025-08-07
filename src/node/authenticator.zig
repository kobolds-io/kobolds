const std = @import("std");
const testing = std.testing;

pub const AuthenticationStrategy = union(AuthenticationStrategyType) {
    const Self = @This();

    none: *NoneAuthStrategy,
    token: *TokenAuthStrategy,
    pub const Context = switch (*Self) {
        .none => NoneAuthStrategy.Context,
        .token => TokenAuthStrategy.Context,
    };

    pub fn authenticate(self: *Self, context: *anyopaque) bool {
        return switch (self.*) {
            .none => |strategy| {
                const c: *NoneAuthStrategy.Context = @ptrCast(@alignCast(context));
                return strategy.authenticate(c.*);
            },
            .token => |strategy| {
                const c: *TokenAuthStrategy.Context = @ptrCast(@alignCast(context));
                return strategy.authenticate(c.*);
            },
        };
    }

    pub fn deinit(self: *Self) void {
        return switch (self.*) {
            .none => |strategy| return strategy.deinit(),
            .token => |strategy| return strategy.deinit(),
        };
    }
};

const AuthenticationStrategyType = enum {
    none,
    token,
};

const NoneAuthStrategy = struct {
    const Self = @This();

    pub const Config = struct {};
    pub const Context = struct {};

    pub fn init() Self {
        return Self{};
    }

    pub fn authenticate(_: *Self, _: Context) bool {
        return true;
    }

    pub fn deinit(_: *Self) void {}
};

const TokenAuthStrategy = struct {
    const Self = @This();

    pub const Config = struct {
        tokens: []const []const u8,
    };

    pub const Context = struct {
        token: []const u8,
    };

    allocator: std.mem.Allocator,
    tokens: *std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const tokens = try allocator.create(std.ArrayList([]const u8));
        errdefer allocator.destroy(tokens);

        tokens.* = try std.ArrayList([]const u8).initCapacity(allocator, config.tokens.len);
        errdefer tokens.deinit();

        try tokens.appendSlice(config.tokens);

        return Self{
            .allocator = allocator,
            .tokens = tokens,
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
        self.allocator.destroy(self.tokens);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;
    _ = allocator;

    var none_auth_strategy = NoneAuthStrategy{};

    // This is just a bullshit test to ensure that we always are able to init/deinit the authenticator
    var authenticator = AuthenticationStrategy{ .none = &none_auth_strategy };
    defer authenticator.deinit();
}

test "none strategy" {
    const allocator = testing.allocator;
    _ = allocator;

    var none_auth_strategy = NoneAuthStrategy{};
    var authenticator = AuthenticationStrategy{ .none = &none_auth_strategy };
    defer authenticator.deinit();

    var context = NoneAuthStrategy.Context{};
    try testing.expectEqual(true, authenticator.authenticate(&context));
}

test "token strategy" {
    const allocator = testing.allocator;

    const allowed_token_1 = "asdf";
    const allowed_token_2 = "1234567890";

    const config = TokenAuthStrategy.Config{
        .tokens = &.{ allowed_token_1, allowed_token_2 },
    };
    var token_auth_strategy = try TokenAuthStrategy.init(allocator, config);

    var authenticator = AuthenticationStrategy{ .token = &token_auth_strategy };
    defer authenticator.deinit();

    var context_1 = TokenAuthStrategy.Context{
        .token = allowed_token_1,
    };

    try testing.expectEqual(true, authenticator.authenticate(&context_1));

    var context_2 = TokenAuthStrategy.Context{
        .token = allowed_token_2,
    };

    try testing.expectEqual(true, authenticator.authenticate(&context_2));

    const bad_token = "some completely random token!";
    var context_3 = TokenAuthStrategy.Context{
        .token = bad_token,
    };
    try testing.expectEqual(false, authenticator.authenticate(&context_3));
}
