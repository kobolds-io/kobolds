const Message = @import("../protocol/message.zig").Message;

pub const Envelope = struct {
    message: *Message,
    conn_id: u64,
    message_id: u64,
    session_id: ?u64 = null,
    worker_id: ?usize = null,
};
