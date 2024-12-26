const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.Worker);

const uuid = @import("uuid");

const constants = @import("../constants.zig");
const Message = @import("../message.zig").Message;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;

// TODO: workers need to understand where a message should be routed. This
// can be done by:
//  - having a reference to all connections
//  - tieing all messages to topics
const Envelope = struct {
    message: *Message,
    destination: uuid.Uuid,
    destination_type: DestinationType,
};

const DestinationType = enum {
    topic,
    service,
    connection,
    node,
};

// recv message
// add message to processing queue
// process_message
//  1. figure out what kind of message it is
//  2. wrap the message in an envelope
//      - if connection -> route message directly to connection
//      - if topic -> route message to topic handler
//      - if service -> route message to service handler
//      - if node -> route message to node handler
//  3.

const WorkerState = enum {
    busy,
    idle,
    closed,
};

/// Worker that processes messages independent of threads
pub const Worker = struct {
    const Self = @This();
    id: u32,

    /// Messages that have been fully processed and are ready to be dispatched
    processed_messages: MessageQueue,

    /// Messages that are yet to be processed
    unprocessed_messages: MessageQueue,

    // maybe this should be a read/write lock instead?
    mutex: std.Thread.Mutex,

    /// state of the worker
    state: WorkerState,

    processed_messages_count: u128,

    pub fn new(id: u32, queue_size: u32) Self {
        return Self{
            .id = id,
            .processed_messages = MessageQueue.new(queue_size),
            .unprocessed_messages = MessageQueue.new(queue_size),
            .mutex = std.Thread.Mutex{},
            .state = .idle,
            .processed_messages_count = 0,
        };
    }

    pub fn tick(self: *Self) !void {
        if (self.unprocessed_messages.count == 0) {
            std.time.sleep(1 * std.time.ns_per_ms);
            return;
        }

        // doing some work!
        std.time.sleep(5 * std.time.ns_per_ms);

        self.processed_messages_count += self.unprocessed_messages.count;

        // FIX: actually process the message
        self.processed_messages.concatenate(&self.unprocessed_messages);
        self.unprocessed_messages.clear();

        // log.debug("worker {} processed {} messages", .{ self.id, self.processed_messages_count });
    }

    pub fn run(self: *Self) void {
        while (true) {
            self.tick() catch unreachable;
        }
    }

    /// Shutdown immediately stops processing any new messages and removes them from the unprocessed queue.
    /// It also changes the state of the worker to a shutdown state which doesn't allow new work to happen.
    /// It is up to the caller to handle the messages that are locked in the processing & unprocessed queues.
    pub fn close(self: *Self) void {
        self.state = .closed;
    }
};
