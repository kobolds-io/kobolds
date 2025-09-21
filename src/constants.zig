const std = @import("std");
const Message = @import("./protocol/message2.zig").Message;
const Headers = @import("./protocol/message.zig").Headers;

/// the maximum size of a message body
pub const message_max_body_size = 1024 * 8; // 8kb

/// The maximum size of a topic.
pub const message_max_topic_name_size = 32; // 32 bytes

/// The maximum size of a message, inluding headers
pub const message_max_size = @sizeOf(Headers) + message_max_body_size;
pub const message_min_size = @sizeOf(Headers);

/// The size of the parser's working buffer to be used for decoding messsages
/// over multiple parse calls.
// pub const parser_max_buffer_size = (1024 * 256) * 2; // TODO: this number needs to be adjusted
// pub const parser_max_buffer_size = (1024 * 64) * 2; // TODO: this number needs to be adjusted
pub const parser_max_buffer_size = (1024 * 32) * 2; // TODO: this number needs to be adjusted
pub const parser_messages_buffer_size = @divFloor(parser_max_buffer_size, @sizeOf(Message));

/// The number of bytes used as the recv buffer in each connection
// pub const connection_recv_buffer_size = (1024 * 256);
// pub const connection_recv_buffer_size = (1024 * 64);
pub const connection_recv_buffer_size = (1024 * 32);

/// The number of bytes used as the send buffer in each connection
// pub const connection_send_buffer_size = (1024 * 256);
// pub const connection_send_buffer_size = (1024 * 64);
pub const connection_send_buffer_size = (1024 * 32);

/// The number of microseconds the IO instance will wait until it flushes
/// submissions and completions.
pub const io_tick_us: u63 = 250;
// pub const io_tick_us: u63 = 100_000; // this is for testing!

/// Number of entries used for the submission and completion queues
pub const io_uring_entries: u16 = 256;

/// Optionally verify if operations based on this boolean.
pub const verify: bool = true;

pub const connection_outbox_capacity: usize = 1_000;
pub const connection_inbox_capacity: usize = 1_000;

pub const topic_max_queue_capacity: usize = 5_000;
pub const publisher_max_queue_capacity: usize = 1_000;
pub const subscriber_max_queue_capacity: usize = 1_000;

pub const service_max_requests_queue_capacity: usize = 1_000;
pub const service_max_replies_queue_capacity: usize = 1_000;
pub const advertiser_max_queue_capacity: usize = 1_000;
pub const requestor_max_queue_capacity: usize = 1_000;

pub const bechmark_iters: u16 = 1;
