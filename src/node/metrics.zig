pub const Metrics = struct {
    messages_processed: u128 = 0,
    last_updated_at_ms: i64 = 0,
    last_printed_at_ms: i64 = 0,
    last_messages_processed_printed: u128 = 0,
};
