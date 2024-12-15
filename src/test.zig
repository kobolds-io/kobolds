// this file is a global importer for all tests
comptime {
    // _ = @import("./benchmarker.zig");
    _ = @import("./hash.zig");
    _ = @import("./io/test.zig");
    // _ = @import("main.zig");
    // _ = @import("./protocol/connection.zig");
    _ = @import("./protocol/message_test.zig");
    _ = @import("./protocol/parser_test.zig");
    _ = @import("./utils.zig");
    _ = @import("./data_structures/message_queue.zig");
    _ = @import("./node.zig");
}
