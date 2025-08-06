const std = @import("std");
const testing = std.testing;

pub const AuthenticatorConfig = struct {
    authentication_strategy: AuthenticationStrategy = .{ .none = {} },
};

pub const AuthenticationStrategy = union(AuthenticationType) {
    auth_token: AuthTokenStrategy,
    none: void,
};

pub const AuthTokenStrategy = struct {
    /// if there are no tokens defined then no one is authorized
    allowed_token: []const u8 = undefined,
};

const AuthenticationType = enum {
    auth_token,
    none,
};

pub const Authenticator = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: AuthenticatorConfig,

    pub fn init(allocator: std.mem.Allocator, config: AuthenticatorConfig) Self {
        return Self{
            .allocator = allocator,
            .config = config,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self;
    }

    pub fn authenticate(self: *Self, context: anytype) bool {
        _ = context;
        switch (self.config.authentication_strategy) {
            .none => return true,
            .auth_token => return false,
        }
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var authenticator = Authenticator.init(allocator, .{});
    defer authenticator.deinit();
}

test "none strategy" {
    const allocator = testing.allocator;

    const config = AuthenticatorConfig{
        .authentication_strategy = AuthenticationStrategy{ .none = {} },
    };

    var authenticator = Authenticator.init(allocator, config);
    defer authenticator.deinit();

    try testing.expectEqual(true, authenticator.authenticate({}));
}
