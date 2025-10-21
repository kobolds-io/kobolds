const std = @import("std");
const Message = @import("./protocol/message.zig").Message;

/// the maximum size of a message body
pub const message_max_body_size = 1024 * 8; // 8kb
// pub const message_max_body_size = 256; // testing only

/// The maximum size of a topic.
pub const message_max_topic_name_size = 32; // 32 bytes
// pub const message_max_topic_name_size = 3; // testing only

/// The maximum size of a message, inluding headers
pub const message_max_size = @sizeOf(Message);

/// The size of the parser's working buffer to be used for decoding messsages
/// over multiple parse calls.
// pub const parser_max_buffer_size = (1024 * 256) * 2; // TODO: this number needs to be adjusted
// pub const parser_max_buffer_size = (1024 * 64) * 2; // TODO: this number needs to be adjusted
pub const parser_max_buffer_size = (1024 * 32) * 2; // TODO: this number needs to be adjusted
// pub const parser_max_buffer_size = @sizeOf(Message) * 2; // TODO: this number needs to be adjusted
// pub const parser_messages_buffer_size = @divFloor(parser_max_buffer_size, @sizeOf(Message));
pub const parser_messages_buffer_size = @divFloor(parser_max_buffer_size, 256);
// pub const parser_messages_buffer_size = 1000;

/// The number of bytes used as the recv buffer in each connection
// pub const connection_recv_buffer_size = (1024 * 256);
// pub const connection_recv_buffer_size = (1024 * 64);
pub const connection_recv_buffer_size = (1024 * 32);
// pub const connection_recv_buffer_size = 300;

// / The number of bytes used as the send buffer in each connection
// pub const connection_send_buffer_size = (1024 * 256);
// pub const connection_send_buffer_size = (1024 * 64);
pub const connection_send_buffer_size = (1024 * 32);
// pub const connection_send_buffer_size = 300;

/// The number of microseconds the IO instance will wait until it flushes
/// submissions and completions.
// pub const io_tick_us: u63 = 1;
// pub const io_tick_us: u63 = 100;
pub const io_tick_us: u63 = 250;
// pub const io_tick_us: u63 = 100_000; // testing only

/// Number of entries used for the submission and completion queues
pub const io_uring_entries: u16 = 256;

/// Optionally verify if operations based on this boolean.
pub const verify: bool = true;

pub const default_node_memory_pool_capacity: usize = 10_000;
pub const default_client_memory_pool_capacity: usize = 5_000;
pub const default_client_outbox_capacity: usize = default_client_memory_pool_capacity;
pub const default_client_inbox_capacity: usize = default_client_memory_pool_capacity;

pub const worker_outbox_capacity: usize = 10_000;
pub const worker_inbox_capacity: usize = 10_000;

pub const connection_outbox_capacity: usize = 5_000;
pub const connection_inbox_capacity: usize = 5_000;

pub const topic_max_queue_capacity: usize = 5_000;
pub const publisher_max_queue_capacity: usize = 5_000;
pub const subscriber_max_queue_capacity: usize = 5_000;

pub const service_max_requests_queue_capacity: usize = 1_000;
pub const service_max_replies_queue_capacity: usize = 1_000;
pub const advertiser_max_queue_capacity: usize = 1_000;
pub const requestor_max_queue_capacity: usize = 1_000;

pub const bechmark_iters: u16 = 1;
