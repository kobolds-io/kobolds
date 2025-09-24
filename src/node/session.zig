const std = @import("std");
const posix = std.posix;
const testing = std.testing;
const assert = std.debug.assert;

const KID = @import("kid").KID;
const IO = @import("../io.zig").IO;

const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message2.zig").Message;
const PeerType = @import("../protocol/connection2.zig").PeerType;
const Connection = @import("../protocol/connection2.zig").Connection;

const LoadBalancer = @import("../load_balancers/load_balancer.zig").LoadBalancer;
const LoadBalancerStrategy = @import("../load_balancers/load_balancer.zig").LoadBalancerStrategy;
const RoundRobin = @import("../load_balancers/load_balancer.zig").RoundRobin;

pub const Session = struct {
    const Self = @This();

    // Identity & ownership
    session_id: u64, // Unique ID for this session (per node)
    peer_id: u64, // Client ID or Node ID (depends on connection type)
    peer_type: PeerType,
    // node_id: u128, // The node that owns this session (local node)

    // // Authentication & security
    session_token: []u8, // Opaque secret issued to peer for resuming
    // auth_method: AuthMethod, // e.g. token, password, public_key
    // id_seed: u64, // Assigned seed for peer-local ID generation

    // Connection management
    connections: std.AutoHashMapUnmanaged(u64, *Connection), // Active TCP connections under this session
    // lease_expiry: i64, // Expiration timestamp (epoch ms)
    // max_connections: u16, // Policy: max connections allowed for this session

    // // Capabilities & permissions
    // features: FeatureSet, // PubSub, KV, WorkerQueue, etc.
    // permissions: PermissionSet, // ACLs per topic/queue/etc.

    // State & tracking
    subscriptions: std.StringHashMapUnmanaged(void),
    // subscriptions: TopicMap, // Active topic subscriptions
    // inflight: MessageMap, // Pending acks, unacked jobs, etc.
    // metadata: Metadata, // Optional client-provided metadata
    load_balancer: LoadBalancer(u64),

    pub fn init(
        allocator: std.mem.Allocator,
        session_id: u64,
        peer_id: u64,
        peer_type: PeerType,
        load_balancing_strategy: LoadBalancerStrategy,
    ) !Self {
        const session_token = try allocator.alloc(u8, 256);
        errdefer allocator.free(session_token);

        var i: usize = 0;
        var salt: [@sizeOf(u64) + @sizeOf(u64)]u8 = undefined;

        std.mem.writeInt(u64, salt[0..@sizeOf(u64)], peer_id, .big);
        i += @sizeOf(u64);
        std.mem.writeInt(u64, salt[i .. i + @sizeOf(u64)][0..@sizeOf(u64)], session_id, .big);
        i += @sizeOf(u64);

        const token = Session.generateSessionToken256(&salt);

        @memcpy(session_token, &token);

        // NOTE: I'm not sure if i would want any other different kind of strategy. Would need to do some research
        // here which would get much more complicated.
        return switch (load_balancing_strategy) {
            .round_robin => Self{
                .session_id = session_id,
                .peer_id = peer_id,
                .peer_type = peer_type,
                .connections = .empty,
                .subscriptions = .empty,
                .session_token = session_token,
                .load_balancer = LoadBalancer(u64){
                    .round_robin = .init(),
                },
            },
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.session_token);

        switch (self.load_balancer) {
            .round_robin => |*lb| lb.deinit(allocator),
        }

        self.connections.deinit(allocator);
        self.subscriptions.deinit(allocator);
    }

    pub fn addConnection(self: *Self, allocator: std.mem.Allocator, conn: *Connection) !void {
        if (self.connections.contains(conn.connection_id)) return error.AlreadyExists;

        try self.connections.put(allocator, conn.connection_id, conn);
        errdefer _ = self.connections.remove(conn.connection_id);

        switch (self.load_balancer) {
            .round_robin => |*lb| try lb.addItem(allocator, conn.connection_id),
        }
    }

    pub fn removeConnection(self: *Self, conn_id: u64) bool {
        const lb_removed = switch (self.load_balancer) {
            .round_robin => |*lb| lb.removeItem(conn_id),
        };

        return self.connections.remove(conn_id) and lb_removed;
    }

    pub fn addSubscription(self: *Self, allocator: std.mem.Allocator, topic_name: []const u8) !void {
        // don't do anything if we are already subscribed. this is for deduplication of subscriptions
        if (self.connections.contains(topic_name)) return;

        try self.subscriptions.put(allocator, topic_name, {});
        errdefer _ = self.subscriptions.remove(topic_name);
    }

    pub fn removeSubscription(self: *Self, topic_name: []const u8) bool {
        return self.subscriptions.remove(topic_name);
    }

    pub fn getNextConnection(self: *Self) *Connection {
        assert(self.connections.count() > 0);

        const next = switch (self.load_balancer) {
            .round_robin => |*lb| lb.next(),
        };

        if (next) |conn_id| {
            return self.connections.get(conn_id).?;
        } else @panic("self.connections and load_balancer.items do not match");
        // NOTE: this means that we are out of sync and have no idea how to deal with this
    }

    pub fn generateSessionToken256(salt: []const u8) [256]u8 {
        // Gather entropy (32 bytes of secure randomness)
        var entropy: [32]u8 = undefined;
        std.crypto.random.bytes(&entropy);

        // Timestamp
        const timestamp = std.time.nanoTimestamp();
        var ts_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &ts_buf, @intCast(timestamp), .big);

        // Concatenate salt + entropy + timestamp
        var buf: [512]u8 = undefined; // large enough scratch
        var buf_len: usize = 0;
        @memcpy(buf[buf_len..salt.len], salt);
        buf_len += salt.len;

        @memcpy(buf[buf_len .. buf_len + entropy.len], &entropy);
        buf_len += entropy.len;

        @memcpy(buf[buf_len .. buf_len + ts_buf.len], &ts_buf);
        buf_len += ts_buf.len;

        // Initialize SHA-512
        var hash = std.crypto.hash.sha2.Sha512.init(.{});
        hash.update(buf[0..buf_len]);

        // Repeatedly hash to fill 256 bytes
        var token: [256]u8 = undefined;
        var digest: [64]u8 = undefined;
        var offset: usize = 0;

        while (offset < token.len) : (offset += digest.len) {
            hash.final(&digest);

            const chunk = token[offset..@min(offset + digest.len, token.len)];
            @memcpy(chunk, digest[0..chunk.len]);

            // prepare next round
            hash = std.crypto.hash.sha2.Sha512.init(.{});
            hash.update(&digest);
        }

        return token;
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    const peer_id = 100;
    const session_id = 100;
    const peer_type = .client;

    var session = try Session.init(allocator, session_id, peer_id, peer_type, .round_robin);
    defer session.deinit(allocator);
}

test "add getNext remove" {
    const allocator = testing.allocator;

    const peer_id = 100;
    const session_id = 100;
    const peer_type = .client;

    var session = try Session.init(allocator, session_id, peer_id, peer_type, .round_robin);
    defer session.deinit(allocator);

    try testing.expectEqual(0, session.connections.count());

    var kid = KID.init(0, .{});

    var io = try IO.init(16, 0);
    defer io.deinit();

    const socket: posix.socket_t = undefined;

    var memory_pool = try MemoryPool(Message).init(allocator, 1_000);
    defer memory_pool.deinit();

    var conn_1 = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, .{ .inbound = .{} });
    defer conn_1.deinit();

    var conn_2 = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, .{ .inbound = .{} });
    defer conn_2.deinit();

    var conn_3 = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, .{ .inbound = .{} });
    defer conn_3.deinit();

    try session.addConnection(allocator, &conn_1);
    try session.addConnection(allocator, &conn_2);
    try session.addConnection(allocator, &conn_3);

    try testing.expectEqual(3, session.connections.count());

    try testing.expectEqual(conn_1.connection_id, session.getNextConnection().connection_id);
    try testing.expectEqual(conn_2.connection_id, session.getNextConnection().connection_id);
    try testing.expectEqual(conn_3.connection_id, session.getNextConnection().connection_id);

    // loops around
    try testing.expectEqual(conn_1.connection_id, session.getNextConnection().connection_id);

    // remove the next conn_id
    try testing.expectEqual(true, session.removeConnection(conn_2.connection_id));

    // ensure we get the expected conn_id
    try testing.expectEqual(conn_3.connection_id, session.getNextConnection().connection_id);
}

test "generate session token" {
    const allocator = testing.allocator;

    const peer_id: u64 = 100;

    var salt: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, salt[0..@sizeOf(u64)], peer_id, .big);

    var tokens: std.StringHashMapUnmanaged(void) = .empty;
    defer tokens.deinit(allocator);

    const iters = 10;

    var i: usize = 0;
    while (i < iters) : (i += 1) {
        const token = try allocator.alloc(u8, 256);
        const session_token = Session.generateSessionToken256(&salt);
        @memcpy(token, &session_token);

        try tokens.put(allocator, token, {});
    }

    try testing.expectEqual(iters, tokens.count());

    var tokens_iter = tokens.keyIterator();
    while (tokens_iter.next()) |entry| {
        const token = entry.*;

        allocator.free(token);
    }
}
