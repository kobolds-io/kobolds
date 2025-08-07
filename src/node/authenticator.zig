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

// pub fn AuthenticationStrategy(comptime T: type) type {
//     return struct {
//         const Self = @This();

//         // pub const Context: T = .{};

//         pub fn authenticate(self: *Self, context: T) bool {

//         }

//         pub fn deinit()
//     };
// }

// pub const Authenticator = struct {
//     const Self = @This();

//     strategy: AuthenticationStrategy,
// };

// pub const Authenticator = struct {
//     const Self = @This();

//     pub const Context = struct {
//         origin_id: u128,
//         connection_id: u128,
//     };

//     /// The interface to be implemented by strategies
//     pub const StrategyInterface = struct {
//         authenticate_fn: *const fn (self: *anyopaque, context: Context) bool,
//         deinit_fn: *const fn (self: *anyopaque) void,
//     };

//     allocator: std.mem.Allocator,
//     strategy: *anyopaque,
//     vtable: *const StrategyInterface,

//     pub fn init(
//         allocator: std.mem.Allocator,
//         comptime T: type,
//         strategy: *T,
//         vtable: *const StrategyInterface,
//     ) Self {
//         return Self{
//             .allocator = allocator,
//             .strategy = strategy,
//             .vtable = vtable,
//         };
//     }

//     pub fn authenticate(self: *Self, context: Context) bool {
//         return self.vtable.authenticate_fn(self.strategy, context);
//     }

//     pub fn deinit(self: *Self) void {
//         self.vtable.deinit_fn(self.strategy);
//     }
// };

// pub const NoneAuthStrategy = struct {
//     const Self = @This();
//     pub const VTable = Authenticator.StrategyInterface{
//         .authenticate_fn = &Self.authenticate,
//         .deinit_fn = &Self.deinit,
//     };

//     pub fn authenticate(_: *anyopaque, _: Authenticator.Context) bool {
//         return true;
//     }

//     pub fn deinit(_: *anyopaque) void {}
// };

// pub const TokenAuthStrategy = struct {
//     const Self = @This();

//     pub const VTable = Authenticator.StrategyInterface{
//         .authenticate_fn = &Self.authenticate,
//         .deinit_fn = &Self.deinit,
//     };

//     const TokenEntry = struct {
//         token: []const u8,
//     };

//     allocator: std.mem.Allocator,
//     allowed_tokens: std.ArrayList(TokenEntry),

//     pub fn init(allocator: std.mem.Allocator) Self {
//         return Self{
//             .allocator = allocator,
//             .allowed_tokens = std.ArrayList(TokenEntry).init(allocator),
//         };
//     }

//     pub fn authenticate(ptr: *anyopaque, _: Authenticator.Context) bool {
//         const self: *Self = @ptrCast(@alignCast(ptr));
//         std.debug.print("self {any}\n", .{self});
//         return true;
//     }

//     pub fn deinit(ptr: *anyopaque) void {
//         const self: *Self = @ptrCast(@alignCast(ptr));

//         self.allowed_tokens.deinit();
//     }
// };

test "init/deinit" {
    // const allocator = testing.allocator;

    // var authenticator = Authenticator.init(allocator, .{});
    // defer authenticator.deinit();
}

test "none strategy" {
    const allocator = testing.allocator;
    _ = allocator;

    var authenticator = Authenticator{ .none = .{} };
    defer authenticator.deinit();
    const context = .{};

    try testing.expectEqual(true, authenticator.authenticate(&context));
}

// test "token strategy" {
//     const allocator = testing.allocator;

//     var token_auth_strategy = TokenAuthStrategy.init(allocator);
//     try token_auth_strategy.allowed_tokens.append(.{ .token = "asdf" });
//     try token_auth_strategy.allowed_tokens.append(.{ .token = "asdfasdf" });

//     var authenticator = Authenticator.init(
//         allocator,
//         TokenAuthStrategy,
//         &token_auth_strategy,
//         &TokenAuthStrategy.VTable,
//     );
//     defer authenticator.deinit();

//     const context = Authenticator.Context{
//         .origin_id = 123,
//         .connection_id = 432,
//     };

//     try testing.expectEqual(true, authenticator.authenticate(context));
// }

// test "auth_token strategy" {
//     const allocator = testing.allocator;

//     const allowed_tokens: [][]const u8 = .{"asdf"};
//     const config = AuthenticatorConfig{
//         .authentication_strategy = AuthenticationStrategy{
//             .auth_token = AuthTokenStrategy{
//                 .allowed_tokens = &allowed_tokens,
//             },
//         },
//     };

//     var authenticator = Authenticator.init(allocator, config);
//     defer authenticator.deinit();

//     const context = Authenticator.Context{
//         .origin_id = 123,
//         .connection_id = 432,
//     };

//     try testing.expectEqual(.auth_token, authenticator.config.authentication_strategy);
//     try testing.expectEqual(true, authenticator.authenticate(context));
// }
