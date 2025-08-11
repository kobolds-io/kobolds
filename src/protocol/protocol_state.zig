pub const ProtocolState = enum {
    inactive,
    awaiting_accept,
    authenticating,
    ready,
    terminating,
    terminated,
};
