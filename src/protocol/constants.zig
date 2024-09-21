const std = @import("std");
const Header = @import("./message.zig").Header;

pub const max_parser_buffer_size = 10 * max_message_size;
pub const max_message_body_size = 1024; // 1024 * 1024; // 1mb
pub const max_message_size = @sizeOf(Header) + max_message_body_size;
pub const connection_read_buffer_size = max_message_size * 4;
pub const connection_write_loop_interval = 10 * std.time.ns_per_ms;
