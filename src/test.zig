// this file is a global importer for all tests
comptime {
    _ = @import("main.zig");
    _ = @import("./proto/parser.zig");
    _ = @import("./proto/message.zig");
}
