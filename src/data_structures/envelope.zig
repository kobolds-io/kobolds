const Message = @import("../protocol/message.zig").Message;
const MemoryPool = @import("stdx").MemoryPool;

pub const Envelope = struct {
    connection_id: u128,
    message: *Message,
};
