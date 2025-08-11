pub const ProtocolState = enum {
    awaiting_accept,
    authenticating,
    ready,
    terminating,
    terminated,
};
