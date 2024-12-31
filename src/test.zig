// this file is a global importer for all tests
comptime {
    // _ = @import("./benchmarker.zig");
    _ = @import("./hash.zig");
    _ = @import("./io/test.zig");
    // _ = @import("main.zig");
    // _ = @import("./connection.zig");
    _ = @import("./message_test.zig");
    _ = @import("./parser_test.zig");
    _ = @import("./utils.zig");
    _ = @import("./data_structures/ring_buffer.zig");
    _ = @import("./data_structures/message_queue.zig");
    _ = @import("./node/service.zig");
    _ = @import("./node/bus.zig");
    _ = @import("./node/node.zig");
    _ = @import("./node/worker.zig");
    _ = @import("./message_pool.zig");
}
