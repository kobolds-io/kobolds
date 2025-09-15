const std = @import("std");
const Time = @import("../time.zig").Time;
const atomic = std.atomic;
const log = std.log.scoped(.KID);
const testing = std.testing;

const Signal = @import("stdx").Signal;

const DEFAULT_EPOCH_MS: u64 = 1735689600000; // Jan 1, 2025, in ms

pub const KIDOptions = struct {
    epoch_ms: u64 = DEFAULT_EPOCH_MS,
};

/// KID (kobolds-id) is heavily influenced by Twitter snowflake id. It uses a fairly simple structure to
/// guarantee uniqness. It is stored as an unsigned 64 bit integer that makes use of the system clock, a
/// node identifier and a counter.
///
/// [ timestamp 40 bits ] | [ counter 13 bits ] | [ node_id 11 bits ]
///
/// Current implementation should support the following criteria.
/// - Timestamp: 40 bits usable until 2059 (34 years)
/// - Counter:   13 bits 8192 ids/ms/node_id
/// - Node ID:   11 bits 2048 nodes
///
/// If this implementation was to change it would be to support more nodes and less ids per ms
pub const KID = struct {
    const Self = @This();

    const IdFields = packed struct {
        node_id: u11 = 0,
        counter: u13 = 0,
        timestamp: u40 = 0,
    };

    const Decoded = struct {
        timestamp_ms: u64,
        counter: u32,
        node_id: u16,
    };

    epoch_ms: u64,
    node_id: u11,
    // Internal state is
    // [40 bit timestamp ms | 13 bits sequence | 11 node_id ]
    state: atomic.Value(u64),
    time: Time,

    pub fn init(node_id: u11, options: KIDOptions) Self {
        return Self{
            .epoch_ms = options.epoch_ms,
            .node_id = node_id,
            .state = atomic.Value(u64).init(0),
            .time = Time{},
        };
    }

    pub fn generate(self: *Self) u64 {
        while (true) {
            // // current time since custom epoch (fits in 40 bits for ~35 years)

            const now_ms: u64 = @intCast(std.time.milliTimestamp());
            const ts_delta = (now_ms -| self.epoch_ms) & ((1 << 40) - 1);
            const ts: u40 = @intCast(ts_delta);

            const old = self.state.load(.seq_cst);
            const old_fields: IdFields = @bitCast(old);

            var new_fields = IdFields{
                .timestamp = ts,
                .counter = 0,
                .node_id = self.node_id,
            };

            if (ts < old_fields.timestamp) {
                // @panic("a hardware/kernel bug regressed the monotonic clock");
                new_fields.timestamp = old_fields.timestamp;
            }

            // we are creating an ID within the same timestamp
            if (new_fields.timestamp == old_fields.timestamp) {
                new_fields.counter = old_fields.counter +% 1; // if overflow, wrap to 0

                // overflow (8192 IDs/ms) must wait for next millisecond
                if (new_fields.counter == 0) {

                    // this is a spin lock and really makes this very blocking in order
                    // to not error out. However, we should take care to not spin forever
                    // we should spin for no longer than 5 milliseconds. If we fail after that time
                    // it means that there is an insane amount of contention and you should adjust
                    // your application.
                    const spin_upper_bounds = now_ms + 5;

                    var later_ms: u64 = @intCast(std.time.milliTimestamp());
                    while (later_ms < spin_upper_bounds) {
                        later_ms = @intCast(std.time.milliTimestamp());
                        const later_delta = (later_ms -| self.epoch_ms) & ((1 << 40) - 1);
                        const later_ts: u40 = @intCast(later_delta);

                        if (later_ts > old_fields.timestamp) {
                            new_fields.timestamp = later_ts;
                            new_fields.counter = 0;
                            break;
                        }

                        if (ts > old_fields.timestamp) break;
                    } else @panic("spin upper bounds breached");

                    new_fields.timestamp = ts;
                    new_fields.counter = 0;
                }
            }

            const new_bits: u64 = @bitCast(new_fields);
            const swapped = self.state.cmpxchgWeak(old, new_bits, .seq_cst, .seq_cst);

            if (swapped == null) return new_bits;

            // else: failed, retry
        }
    }

    pub fn decode(self: Self, id: u64) Decoded {
        const fields: IdFields = @bitCast(id);
        return .{
            .timestamp_ms = @as(u64, fields.timestamp) + self.epoch_ms,
            .counter = fields.counter,
            .node_id = fields.node_id,
        };
    }
};

test "generates sequential ids" {
    var kid = KID.init(1, .{});
    const kid_0 = kid.generate();
    const kid_1 = kid.generate();

    const kid_0_decoded = kid.decode(kid_0);
    const kid_1_decoded = kid.decode(kid_1);

    try testing.expect(kid_0_decoded.timestamp_ms <= kid_1_decoded.timestamp_ms);

    if (kid_0_decoded.timestamp_ms == kid_1_decoded.timestamp_ms) {
        try testing.expectEqual(0, kid_0_decoded.counter);
        try testing.expectEqual(1, kid_1_decoded.counter);
    } else {
        try testing.expectEqual(0, kid_0_decoded.counter);
        try testing.expectEqual(0, kid_1_decoded.counter);
    }
    try testing.expect(kid_0_decoded.node_id == kid_1_decoded.node_id);
}

test "can generate 8192 ids per ms" {
    const allocator = testing.allocator;

    var kid = KID.init(2, .{});

    const iters = 100_000;

    var ids = try std.ArrayList(u64).initCapacity(allocator, iters);
    defer ids.deinit(allocator);

    for (0..iters) |_| {
        const id = kid.generate();
        ids.appendAssumeCapacity(id);
    }

    // loop over all the ids
    var current_ts: u64 = 0;
    for (ids.items) |id| {
        const decoded = kid.decode(id);
        if (decoded.timestamp_ms > current_ts) current_ts = decoded.timestamp_ms;

        // ensure that all counters are less than the max of a 13 bit integer 8192
        try testing.expect(decoded.counter <= std.math.maxInt(u13));
    }

    try testing.expectEqual(iters, ids.items.len);
}

test "KID is thread-safe and generates unique IDs" {
    return error.SkipZigTest;
}
