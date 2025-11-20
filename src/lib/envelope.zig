const Message = @import("./message.zig").Message;

pub const Envelope = struct {
    /// reference to the message itself
    message: *Message,
    /// indentifier tying this message to the session of the sender
    session_id: u64,
    /// broker assigned identifier of this message
    message_id: u64,
    // TODO: add more fields here to help with routing this message
};
