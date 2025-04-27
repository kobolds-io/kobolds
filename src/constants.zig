const std = @import("std");
const Message = @import("./protocol/message.zig").Message;
const Headers = @import("./protocol/message.zig").Headers;

/// the maximum size of a message body
pub const message_max_body_size = 1024 * 8; // 8kb

/// The maximum size of a topic.
pub const message_max_topic_name_size = 32; // 32 bytes

/// The maximum size of a message, inluding headers
pub const message_max_size = @sizeOf(Headers) + message_max_body_size;

/// The size of the parser's working buffer to be used for decoding messsages
/// over multiple parse calls.
pub const parser_max_buffer_size = message_max_size * 10; // TODO: this number needs to be adjusted

/// The number of bytes used as the recv buffer in each connection
pub const connection_recv_buffer_size = 4096;

/// The number of bytes used as the send buffer in each connection
pub const connection_send_buffer_size = 4096;

/// The number of iterations to be executed for benchmark tests
pub const benchmark_testing_iterations = 3_500_000;

/// The number of milliseconds the IO instance will wait until it flushes
/// submissions and completions.
pub const io_tick_ms: u63 = 1;

/// Number of entries used for the submission and completion queues
pub const io_uring_entries: u16 = 256;

/// Optionally verify if operations based on this boolean.
pub const verify: bool = true;

/// Maximum size of the message pool
pub const default_worker_message_pool_capacity: u32 = 10_000;

pub const connection_outbox_capacity: u32 = 5_000;
pub const connection_inbox_capacity: u32 = 5_000;

pub const publisher_max_queue_capacity: u32 = 1_000;
pub const subscriber_max_queue_capacity: u32 = 5_000;
