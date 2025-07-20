const Message = @import("../protocol/message.zig").Message;

pub const Envelope = struct {
    worker_id: usize,
    receiver_id: u128,
    sender_id: u128,
    message: *Message,
};
