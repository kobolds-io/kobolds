// this file is a global importer for all tests
comptime {
    // _ = @import("main.zig");
    // _ = @import("./protocol/connection.zig");
    _ = @import("./protocol/hash_test.zig");
    _ = @import("./protocol/message_test.zig");
    _ = @import("./protocol/parser_test.zig");
    _ = @import("./protocol/utils_test.zig");
    // _ = @import("./benchmarker.zig");
}
