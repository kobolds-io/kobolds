/// Peer is the central construct for interacting with Kobolds.
pub const Peer = struct {
    const Self = @This();
    pub const Config = struct {};
};

// const peer = try Peer.init(allocator, config)
// defer peer.deinit(allocator);
//
// session_id = try peer.connect(host, port, connections, etc);
// defer peer.disconnect(session_id);
//
// try peer.publish(topic_name, body, opts);
