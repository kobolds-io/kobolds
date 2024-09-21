// this file is a global importer for all tests
comptime {
    _ = @import("main.zig");
    _ = @import("./protocol/parser.zig");
    _ = @import("./protocol/message.zig");
    _ = @import("./protocol/hash.zig");
    _ = @import("./protocol/connection.zig");
    _ = @import("./protocol/message_bus.zig");
}
