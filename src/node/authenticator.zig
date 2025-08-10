const std = @import("std");
const testing = std.testing;

const AuthenticationStrategyType = enum {
    none,
    token,
};

const NoneAuthStrategy = struct {
    const Self = @This();

    pub const Context = struct {};
    pub const Config = struct {};

    pub fn init(_: Config) Self {
        return .{};
    }

    pub fn authenticate(_: *Self, _: Self.Context) bool {
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

pub fn Authenticator(comptime T: AuthenticationStrategyType) type {
    return switch (T) {
        .none => NoneAuthStrategy,
        .token => TokenAuthStrategy,
    };
}

test "init/deinit" {
    const auth_strategy: AuthenticationStrategyType = .none;
    var authenticator = Authenticator(auth_strategy).init(.{});
    defer authenticator.deinit();
}

test "none strategy" {
    const AuthenticatorType = Authenticator(.none);
    var authenticator = AuthenticatorType.init(.{});
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

test "figuring out what strategy" {
    const auth_strategy: AuthenticationStrategyType = .none;
    var authenticator = Authenticator(auth_strategy).init(.{});
    defer authenticator.deinit();

    // ... some time later
    switch (@TypeOf(authenticator)) {
        NoneAuthStrategy => std.debug.print("none auth strategy!", .{}),
        TokenAuthStrategy => std.debug.print("token auth strategy!", .{}),
        else => std.debug.print("unknown auth strategy!", .{}),
    }
}
