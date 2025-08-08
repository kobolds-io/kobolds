const std = @import("std");
const testing = std.testing;

pub const AuthenticationStrategy = union(AuthenticationStrategyType) {
    const Self = @This();

    none: NoneAuthStrategy,
    token: TokenAuthStrategy,

    pub const Context = switch (*Self) {
        .none => NoneAuthStrategy.Context,
        .token => TokenAuthStrategy.Context,
    };

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

pub const AuthenticatorConfig = struct {
    const Self = @This();
    strategy_type: AuthenticationStrategyType = .none,
    none: ?NoneAuthStrategy = .{},
    token: ?TokenAuthStrategy = null,

    pub fn validate(self: Self) ?[]const u8 {
        switch (self.strategy_type) {
            .none => {
                if (self.none == null) return "`AuthenticatorConfig` none must be configured";
            },
            .token => {
                if (self.token == null) return "`AuthenticatorConfig` token must be configured";
            },
        }

        return null;
    }
};

pub fn Authenticator(comptime T: AuthenticationStrategyType) type {
    return switch (T) {
        .none => struct {
            const Self = @This();

            pub const Context = NoneAuthStrategy.Context;
            pub const Config = NoneAuthStrategy.Config;

            pub fn init() Self {
                return .{};
            }

            pub fn authenticate(_: *Self, _: Self.Context) bool {
                return true;
            }

            pub fn deinit(_: *Self) void {}
        },
        .token => struct {
            const Self = @This();

            pub const Context = TokenAuthStrategy.Context;
            pub const Config = TokenAuthStrategy.Config;

            allocator: std.mem.Allocator,
            strategy: *TokenAuthStrategy,

            pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
                const strategy = try allocator.create(TokenAuthStrategy);
                errdefer allocator.destroy(strategy);

                strategy.* = try TokenAuthStrategy.init(allocator, config);
                errdefer strategy.deinit();

                return Self{
                    .allocator = allocator,
                    .strategy = strategy,
                };
            }

            pub fn deinit(self: *Self) void {
                self.strategy.deinit();
                self.allocator.destroy(self.strategy);
            }

            pub fn authenticate(self: *Self, context: Context) bool {
                for (self.strategy.tokens.items) |token| {
                    if (std.mem.eql(u8, token, context.token)) return true;
                }

                return false;
            }
        },
    };
}

test "init/deinit" {
    const AuthenticatorType = Authenticator(.none);

    var authenticator = AuthenticatorType.init();
    defer authenticator.deinit();
}

test "none strategy" {
    const AuthenticatorType = Authenticator(.none);

    var authenticator = AuthenticatorType.init();
    defer authenticator.deinit();

    const context = AuthenticatorType.Context{};
    try testing.expectEqual(true, authenticator.authenticate(context));
}

test "token strategy" {
    const allocator = testing.allocator;

    const allowed_token_1 = "asdf";
    const allowed_token_2 = "1234567890";

    const AuthenticatorType = Authenticator(.token);
    const config = AuthenticatorType.Config{
        .tokens = &.{ allowed_token_1, allowed_token_2 },
    };

    var authenticator = try AuthenticatorType.init(allocator, config);
    defer authenticator.deinit();

    const context_1 = AuthenticatorType.Context{
        .token = allowed_token_1,
    };

    try testing.expectEqual(true, authenticator.authenticate(context_1));

    const context_2 = AuthenticatorType.Context{
        .token = allowed_token_2,
    };

    try testing.expectEqual(true, authenticator.authenticate(context_2));

    const bad_token = "some completely random token!";
    const context_3 = AuthenticatorType.Context{
        .token = bad_token,
    };
    try testing.expectEqual(false, authenticator.authenticate(context_3));
}
