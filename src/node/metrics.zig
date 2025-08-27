const std = @import("std");
const atomic = std.atomic;

pub const NodeMetrics = struct {
    messages_processed: atomic.Value(i64) = atomic.Value(i64).init(0),
    last_updated_at_ms: i64 = 0,
    last_printed_at_ms: i64 = 0,
    last_messages_processed_printed: i64 = 0,
    bytes_processed: i64 = 0,
    last_bytes_processed_printed: i64 = 0,
};
