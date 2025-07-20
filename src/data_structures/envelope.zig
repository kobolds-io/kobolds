const Message = @import("../protocol/message.zig").Message;

pub const Envelope = struct {
    worker_id: usize,
    connection_id: u128,
    message: *Message,
};
