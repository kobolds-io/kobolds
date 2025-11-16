const std = @import("std");

/// A magical resync value "KB" (kobold)
pub const frame_headers_magic: u16 = 0x4B42;

/// size of each chunk.data used for assembling messages
pub const message_chunk_data_size: usize = 1024 * 4;

/// Maximum size of a frame's payload
pub const max_frame_payload_size: usize = std.math.maxInt(u16);
