pub const ProtocolState = enum {
    inactive, // Client/Node
    sending_accept, // Node
    awaiting_accept, // Client
    sending_challenge, // Node
    awaiting_challenge, // Client
    awaiting_credentials, // Node
    sending_credentials, // Client
    awaiting_authentication, // Client
    ready, // Client/Node
    terminating, // Client/Node
    terminated, // Client/Node
};

// node perpective
// 1. inactive (inital state) -> sending accept
// 2. sending accept -> sending auth challenge
// 3. sending auth challenge -> awaiting_auth_credentials/terminating
// 4. awaiting auth credentials -> authenticating/terminating
// 5. authenticating -> ready/terminating
// 6. ready
// 7. terminating
// 8. terminated
//
// client perpective
// 1. inactive (inital state)
// 2. awaiting accept
// 3. awaiting auth challenge
// 4. sending auth credentials
// 5. awaiting auth reply
// 6. ready
// 7. terminating
// 8. terminated
