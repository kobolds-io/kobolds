pub const MessageType = enum(u8) {
    unsupported,
    // auth_challenge,
    // session_init,
    // session_join,
    // auth_failure,
    // auth_success,
    // publish,
    // subscribe,
    // subscribe_ack,
    // unsubscribe,
    // unsubscribe_ack,
    // service_request,
    // service_reply,
    // advertise,
    // advertise_ack,

    pub fn fromByte(byte: u8) MessageType {
        return switch (byte) {
            1 => .auth_challenge,
            // 2 => .session_init,
            // 3 => .session_join,
            // 4 => .auth_failure,
            // 5 => .auth_success,
            // 6 => .publish,
            // 7 => .subscribe,
            // 8 => .subscribe_ack,
            // 9 => .unsubscribe,
            // 10 => .unsubscribe_ack,
            // 11 => .service_request,
            // 12 => .service_reply,
            // 13 => .advertise,
            // 14 => .advertise_ack,
            else => .unsupported,
        };
    }
};
