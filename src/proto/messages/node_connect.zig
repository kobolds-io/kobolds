const Headers = @import("./headers.zig");

const NodeConnectMessage = struct {
    id: []const u8,
    headers: Headers,
};
