const std = @import("std");
const testing = std.testing;
const ChallengeMethod = @import("../protocol/message.zig").ChallengeMethod;

const AuthenticationStrategyType = ChallengeMethod;

pub const NoneAuthStrategy = struct {
    const Self = @This();

    pub const Context = struct {};
    pub const Config = struct {};

    pub const Challenge = struct {
        challenge_method: AuthenticationStrategyType.none,
    };

    pub const ChallengeContext = struct {};

    pub fn init(_: std.mem.Allocator, _: Config) Self {
        return .{};
    }

    pub fn authenticate(_: *Self, _: *const Context) bool {
        return true;
    }

    pub fn challenge(_: *Self, _: ChallengeContext) Challenge {
        return Challenge{
            .challenge_method = .none,
        };
    }

    pub fn deinit(_: *Self) void {}
};

pub const TokenAuthStrategy = struct {
    const Self = @This();

    pub const Config = struct {
        tokens: []const []const u8,
    };

    pub const Context = struct {
        token: []const u8,
    };

    pub const Challenge = struct {
        challenge_method: AuthenticationStrategyType.token,
    };

    pub const ChallengeContext = struct {};

    allocator: std.mem.Allocator,
    tokens: *std.array_list.Managed([]const u8),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const tokens = try allocator.create(std.array_list.Managed([]const u8));
        errdefer allocator.destroy(tokens);

        tokens.* = try std.array_list.Managed([]const u8).initCapacity(allocator, config.tokens.len);
        errdefer tokens.deinit();

        try tokens.appendSlice(config.tokens);

        return Self{
            .allocator = allocator,
            .tokens = tokens,
        };
    }

    pub fn authenticate(self: *Self, context: *const Context) bool {
        for (self.tokens.items) |token| {
            if (std.mem.eql(u8, token, context.token)) return true;
        }

        return false;
    }

    pub fn challenge(_: *Self, _: ChallengeContext) Challenge {
        return Challenge{
            .challenge_method = .token,
        };
    }

    pub fn deinit(self: *Self) void {
        self.tokens.deinit();
        self.allocator.destroy(self.tokens);
    }
};

pub const AuthenticatorConfig = union(AuthenticationStrategyType) {
    none: NoneAuthStrategy.Config,
    token: TokenAuthStrategy.Config,
};

pub const Authenticator = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    strategy_type: AuthenticationStrategyType,
    strategy: StrategyUnion,

    const StrategyUnion = union(AuthenticationStrategyType) {
        none: NoneAuthStrategy,
        token: TokenAuthStrategy,
    };

    pub fn init(allocator: std.mem.Allocator, config: AuthenticatorConfig) !Self {
        return switch (config) {
            .none => .{
                .allocator = allocator,
                .strategy_type = .none,
                .strategy = .{ .none = NoneAuthStrategy.init(allocator, config.none) },
            },
            .token => .{
                .allocator = allocator,
                .strategy_type = .token,
                .strategy = .{ .token = try TokenAuthStrategy.init(allocator, config.token) },
            },
        };
    }

    pub fn deinit(self: *Self) void {
        switch (self.strategy) {
            .none => |*s| s.deinit(),
            .token => |*s| s.deinit(),
        }
    }

    pub fn authenticate(self: *Self, context: *const anyopaque) bool {
        return switch (self.strategy) {
            .none => |*s| {
                const ctx: *const NoneAuthStrategy.Context = @ptrCast(@alignCast(context));
                return s.authenticate(ctx);
            },
            .token => |*s| {
                const ctx: *const TokenAuthStrategy.Context = @ptrCast(@alignCast(context));
                return s.authenticate(ctx);
            },
        };
    }

    pub fn getChallengeMethod(self: *Self) ChallengeMethod {
        return switch (self.strategy) {
            .none => return .none,
            .token => return .token,
        };
    }

    pub fn getChallengePayload(self: *Self) []const u8 {
        return switch (self.strategy) {
            .none => return "",
            .token => return "",
        };
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var none_authenticator = try Authenticator.init(allocator, .{ .none = .{} });
    defer none_authenticator.deinit();
}

test "none strategy" {
    const allocator = testing.allocator;

    var none_authenticator = try Authenticator.init(allocator, .{ .none = .{} });
    defer none_authenticator.deinit();

    var context = NoneAuthStrategy.Context{};
    try testing.expectEqual(true, none_authenticator.authenticate(&context));
}

test "token strategy" {
    const allocator = testing.allocator;

    const allowed_token_1 = "asdf";
    const allowed_token_2 = "1234567890";

    const token_authenticator_config = TokenAuthStrategy.Config{
        .tokens = &[_][]const u8{ allowed_token_1, allowed_token_2 },
    };
    var authenticator = try Authenticator.init(allocator, .{ .token = token_authenticator_config });
    defer authenticator.deinit();

    const context_1 = TokenAuthStrategy.Context{ .token = allowed_token_1 };
    try testing.expectEqual(true, authenticator.authenticate(&context_1));

    const context_2 = TokenAuthStrategy.Context{ .token = allowed_token_2 };
    try testing.expectEqual(true, authenticator.authenticate(&context_2));

    const bad_token = "some completely random token!";
    const context_3 = TokenAuthStrategy.Context{ .token = bad_token };
    try testing.expectEqual(false, authenticator.authenticate(&context_3));
}
