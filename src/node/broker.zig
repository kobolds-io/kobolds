const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Broker);
const posix = std.posix;

const uuid = @import("uuid");

const constants = @import("../constants.zig");

const Service = @import("./service.zig").Service;
const Topic = @import("./topic.zig").Topic;
const IO = @import("../io.zig").IO;
const TopicManager = @import("./topic_manager.zig").TopicManager;

// Protocol
const Connection = @import("../protocol/connection.zig").Connection;
const Message = @import("../protocol/message.zig").Message;
const Request = @import("../protocol/message.zig").Request;
const Ping = @import("../protocol/message.zig").Ping;
const Pong = @import("../protocol/message.zig").Pong;
const Accept = @import("../protocol/message.zig").Accept;

// Datastructures
const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const MessagePool = @import("../data_structures/message_pool.zig").MessagePool;
const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;
const EventEmitter = @import("../data_structures/event_emitter.zig").EventEmitter;

pub const BrokerConfig = struct {};

pub const Broker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    config: BrokerConfig,
    connection_messages: *ConnectionMessages,
    connection_messages_mutex: std.Thread.Mutex,
    message_pool: *MessagePool,
    topic_manager: *TopicManager,
    processed_messages_count: u128,
    pending_messages: MessageQueue,
    pending_messages_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, message_pool: *MessagePool, topic_manager: *TopicManager, config: BrokerConfig) !Self {
        const connection_messages = try allocator.create(ConnectionMessages);
        errdefer allocator.destroy(connection_messages);

        connection_messages.* = ConnectionMessages.init(allocator);
        errdefer connection_messages.deinit();

        // const topic_manager = try allocator.create(TopicManager);
        // errdefer allocator.destroy(topic_manager);
        //
        // topic_manager.* = TopicManager.init(allocator, message_pool);
        // errdefer topic_manager.deinit();

        return Self{
            .allocator = allocator,
            .config = config,
            .connection_messages = connection_messages,
            .connection_messages_mutex = std.Thread.Mutex{},
            .message_pool = message_pool,
            .topic_manager = topic_manager,
            .processed_messages_count = 0,
            .pending_messages = MessageQueue.new(),
            .pending_messages_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.connection_messages.deinit();
        // self.topic_manager.deinit();

        // Destroy references
        self.allocator.destroy(self.connection_messages);
        // self.allocator.destroy(self.topic_manager);
    }

    pub fn tick(self: *Self) !void {
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // const start = timer.read();
        // const messages_already_processed: u128 = self.processed_messages_count;
        // defer {
        //     const end = timer.read();
        //     const took = ((end - start) / std.time.ns_per_us);
        //
        //     log.debug("tick: {d:6}us, processed_tick: {d:6} processed_total: {d:8}, free: {d:6}, ", .{
        //         took,
        //         self.processed_messages_count - messages_already_processed,
        //         self.processed_messages_count,
        //         self.message_pool.available(),
        //     });
        // }

        // try self.processPendingMessages();

        try self.topic_manager.tick();
    }

    fn processPendingMessages(self: *Self) !void {
        if (self.pending_messages.count == 0) return;
        log.debug("here", .{});

        while (self.pending_messages.dequeue()) |message| {
            defer self.processed_messages_count += 1;

            {
                defer message.deref();

                switch (message.headers.message_type) {
                    .ping => try self.pingHandler(message),
                    .publish => try self.publishHandler(message),
                    else => unreachable,
                }
            }

            if (message.refs() == 0) self.message_pool.destroy(message);
        }
    }

    fn pingHandler(self: *Self, message: *Message) !void {
        const transaction_id = message.transactionId();
        // we hijack this message so we don't have to do another allocation
        message.headers.message_type = .pong;
        message.setTransactionId(transaction_id);
        message.setErrorCode(.ok);

        self.connection_messages_mutex.lock();
        defer self.connection_messages_mutex.unlock();
        try self.connection_messages.append(message.headers.origin_id, message);

        // reference this message so it doesn't get cleaned up later
        message.ref();
    }

    fn publishHandler(self: *Self, message: *Message) !void {
        _ = self;
        _ = message;
    }
};
