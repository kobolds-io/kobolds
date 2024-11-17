const std = @import("std");
const Headers = @import("./message.zig").Headers;

pub const message_max_body_size = 1024 * 8; // 8kb
pub const message_max_topic_size = 32; // 32 bytes
pub const message_max_size = @sizeOf(Headers) + message_max_body_size;
pub const parser_max_buffer_size = 10 * message_max_size;
pub const connection_read_buffer_size = message_max_size * 4;
pub const connection_write_loop_interval = 10 * std.time.ns_per_ms;

/// The number of iterations to be executed for benchmark tests
pub const benchmark_testing_iterations = 3_500_000;
