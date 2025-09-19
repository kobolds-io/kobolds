const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const PeerType = @import("../protocol/connection2.zig").PeerType;
const RoundRobinLoadBalancer = @import("../services/load_balancers.zig").RoundRobinLoadBalancer;

pub const SessionLoadBalancer = union(SessionLoadBalancingStrategy) {
    round_robin: RoundRobinLoadBalancer(u64),
};

pub const SessionLoadBalancingStrategy = enum {
    round_robin,
};

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
    connections: std.AutoHashMapUnmanaged(u64, void), // Active TCP connections under this session
    // lease_expiry: i64, // Expiration timestamp (epoch ms)
    // max_connections: u16, // Policy: max connections allowed for this session

    // // Capabilities & permissions
    // features: FeatureSet, // PubSub, KV, WorkerQueue, etc.
    // permissions: PermissionSet, // ACLs per topic/queue/etc.

    // // State & tracking
    // subscriptions: TopicMap, // Active topic subscriptions
    // inflight: MessageMap, // Pending acks, unacked jobs, etc.
    // metadata: Metadata, // Optional client-provided metadata
    load_balancer: SessionLoadBalancer,

    // QUESTION: do i need a mutex for adding/removing connections??

    pub fn init(
        allocator: std.mem.Allocator,
        session_id: u64,
        peer_id: u64,
        peer_type: PeerType,
        session_load_balancing_strategy: SessionLoadBalancingStrategy,
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
        switch (session_load_balancing_strategy) {
            .round_robin => {
                return Self{
                    .session_id = session_id,
                    .peer_id = peer_id,
                    .peer_type = peer_type,
                    .connections = .empty,
                    .session_token = session_token,
                    .load_balancer = SessionLoadBalancer{
                        .round_robin = RoundRobinLoadBalancer(u64).init(),
                    },
                };
            },
        }
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.session_token);

        switch (self.load_balancer) {
            .round_robin => |*lb| lb.deinit(allocator),
        }

        self.connections.deinit(allocator);
    }

    pub fn addConnection(self: *Self, allocator: std.mem.Allocator, conn_id: u64) !void {
        if (self.connections.get(conn_id)) |_| {
            return error.AlreadyExists;
        }

        try self.connections.put(allocator, conn_id, {});
        errdefer _ = self.connections.remove(conn_id);

        switch (self.load_balancer) {
            .round_robin => |*lb| {
                try lb.keys.append(allocator, conn_id);
                errdefer _ = lb.keys.pop();

                std.mem.sort(u64, lb.keys.items, {}, std.sort.asc(u64));
            },
        }
    }

    pub fn removeConnection(self: *Self, conn_id: u64) bool {
        switch (self.load_balancer) {
            .round_robin => |*lb| {
                for (lb.keys.items, 0..lb.keys.items.len) |k, index| {
                    if (k == conn_id) {
                        // NOTE: I'm like 99% sure that we don't need to resort the list at all if we do an ordered remove
                        //     i'm dumb though
                        _ = lb.keys.orderedRemove(index);
                        break;
                    }
                }
            },
        }

        return self.connections.remove(conn_id);
    }

    pub fn getNextConnection(self: *Self) u64 {
        assert(self.connections.count() > 0);

        switch (self.load_balancer) {
            .round_robin => |*lb| {
                assert(lb.keys.items.len > 0);

                lb.current_index = @min(lb.keys.items.len - 1, lb.current_index);
                const conn_id = lb.keys.items[lb.current_index];

                lb.current_index = (lb.current_index + 1) % lb.keys.items.len;

                // if there is no key here then something is borked. just explode
                return self.connections.getKey(conn_id).?;
            },
        }

        unreachable;
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

    const conn_id_1 = 10;
    const conn_id_2 = 11;
    const conn_id_3 = 12;

    try session.addConnection(allocator, conn_id_1);
    try session.addConnection(allocator, conn_id_2);
    try session.addConnection(allocator, conn_id_3);

    try testing.expectEqual(3, session.connections.count());

    try testing.expectEqual(conn_id_1, session.getNextConnection());
    try testing.expectEqual(conn_id_2, session.getNextConnection());
    try testing.expectEqual(conn_id_3, session.getNextConnection());

    // loops around
    try testing.expectEqual(conn_id_1, session.getNextConnection());

    // remove the next conn_id
    try testing.expectEqual(true, session.removeConnection(conn_id_2));

    // ensure we get the expected conn_id
    try testing.expectEqual(conn_id_3, session.getNextConnection());
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
