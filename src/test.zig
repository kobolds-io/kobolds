// this file is a global importer for all tests
comptime {
    _ = @import("./bus/bus_manager.zig");
    _ = @import("./bus/bus.zig");
    _ = @import("./bus/publisher.zig");
    _ = @import("./bus/subscriber.zig");
    _ = @import("./data_structures/channel.zig");
    _ = @import("./data_structures/connection_messages.zig");
    _ = @import("./data_structures/event_emitter.zig");
    // _ = @import("./data_structures/message_pool.zig");
    _ = @import("./hash.zig");
    _ = @import("./io/test.zig");
    _ = @import("./node/listener.zig");
    _ = @import("./node/node.zig");
    _ = @import("./node/worker.zig");
    _ = @import("./protocol/message_test.zig");
    _ = @import("./protocol/parser_test.zig");
    _ = @import("./utils.zig");
}
