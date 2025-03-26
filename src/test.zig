// this file is a global importer for all tests
comptime {
    _ = @import("./data_structures/channel.zig");
    _ = @import("./data_structures/connection_messages.zig");
    _ = @import("./data_structures/event_emitter.zig");
    _ = @import("./data_structures/managed_queue.zig");
    _ = @import("./data_structures/memory_pool.zig");
    // _ = @import("./data_structures/message_pool.zig");
    _ = @import("./data_structures/message_queue.zig");
    _ = @import("./data_structures/resource_pool.zig");
    _ = @import("./data_structures/ring_buffer.zig");
    _ = @import("./data_structures/unmanaged_queue.zig");
    _ = @import("./hash.zig");
    _ = @import("./io/test.zig");
    _ = @import("./node/acceptor.zig");
    _ = @import("./node/broker.zig");
    _ = @import("./node/node.zig");
    _ = @import("./node/service.zig");
    _ = @import("./node/topic_manager.zig");
    _ = @import("./node/topic.zig");
    _ = @import("./node/subscriber.zig");
    _ = @import("./node/topic_2.zig");
    _ = @import("./node/worker.zig");
    _ = @import("./protocol/message_test.zig");
    _ = @import("./protocol/parser_test.zig");
    _ = @import("./utils.zig");
}
