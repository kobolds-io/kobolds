// this file is a global importer for all tests
comptime {
    _ = @import("./client/client.zig");
    _ = @import("./data_structures/connection_messages.zig");
    _ = @import("./data_structures/message_pool.zig");
    _ = @import("./hash.zig");
    _ = @import("./io/test.zig");
    _ = @import("./node/authenticator.zig");
    _ = @import("./node/listener.zig");
    _ = @import("./node/node.zig");
    _ = @import("./node/worker.zig");
    _ = @import("./protocol/message2.zig");
    _ = @import("./protocol/parser2.zig");
    _ = @import("./protocol/message_test.zig");
    _ = @import("./protocol/parser_test.zig");
    _ = @import("./utils.zig");
}
