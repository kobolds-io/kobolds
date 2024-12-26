const std = @import("std");
const Headers = @import("./message.zig").Headers;

/// the maximum size of a message body
pub const message_max_body_size = 1024 * 8; // 8kb

/// The maximum size of a topic.
pub const message_max_topic_size = 32; // 32 bytes

/// The maximum size of a message, inluding headers
pub const message_max_size = @sizeOf(Headers) + message_max_body_size;

/// The size of the parser's working buffer to be used for decoding messsages
/// over multiple parse calls.
pub const parser_max_buffer_size = 10 * message_max_size;

/// The number of bytes used as the recv buffer in each connection
pub const connection_recv_buffer_size = 4096;

/// The number of bytes used as the send buffer in each connection
pub const connection_send_buffer_size = 4096;

/// The number of iterations to be executed for benchmark tests
pub const benchmark_testing_iterations = 3_500_000;

/// The number of milliseconds the IO instance will wait until it flushes
/// submissions and completions from.
pub const io_tick_ms: u63 = 10;

/// Number of entries used for the submission and completion queues
pub const io_uring_entries: u16 = 256;

/// Optionally verify if operations based on this boolean.
pub const verify: bool = true;

/// Maximum size of a queue
pub const queue_size_max: u32 = 100_000;

/// Default size of a queue when no size is provided
pub const queue_size_default: u32 = 10_000;

/// Maximum size of the message pool
pub const message_pool_max_size: u32 = 100_000;

/// Default number of workers spawned
pub const worker_default_threads: u32 = 10;
