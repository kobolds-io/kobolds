pub const PeerType = enum {
    client,
    node,
};

pub const Session = struct {
    // Identity & ownership
    session_id: u64, // Unique ID for this session (per node)
    peer_id: ?[]const u8, // Client ID or Node ID (depends on connection type)
    peer_type: PeerType,
    // node_id: u128, // The node that owns this session (local node)

    // // Authentication & security
    // session_token: []const u8, // Opaque secret issued to peer for resuming
    // auth_method: AuthMethod, // e.g. token, password, public_key
    // id_seed: u64, // Assigned seed for peer-local ID generation

    // // Connection management
    // connections: ConnectionMap, // Active TCP connections under this session
    // lease_expiry: i64, // Expiration timestamp (epoch ms)
    // max_connections: u16, // Policy: max connections allowed for this session

    // // Capabilities & permissions
    // features: FeatureSet, // PubSub, KV, WorkerQueue, etc.
    // permissions: PermissionSet, // ACLs per topic/queue/etc.

    // // State & tracking
    // subscriptions: TopicMap, // Active topic subscriptions
    // inflight: MessageMap, // Pending acks, unacked jobs, etc.
    // metadata: Metadata, // Optional client-provided metadata
};
