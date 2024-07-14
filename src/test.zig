// this file is a global importer for all tests
comptime {
    _ = @import("main.zig");
    _ = @import("./proto/parser.zig");
    _ = @import("./proto/headers.zig");
    _ = @import("./proto/message.zig");
    _ = @import("./proto/proto.zig");
    _ = @import("./proto/utils.zig");
    _ = @import("./proto/node.zig");
}
