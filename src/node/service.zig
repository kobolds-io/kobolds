const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.Service);

const uuid = @import("uuid");
const constants = @import("../constants.zig");

const Message = @import("../protocol/message.zig").Message;
const Request = @import("../protocol/message.zig").Request;
const Reply = @import("../protocol/message.zig").Reply;

const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const ConnectionMessages = @import("../data_structures/connection_messages.zig").ConnectionMessages;

pub const Transaction = struct {
    requestor: uuid.Uuid,
    replier: uuid.Uuid,
};

pub const Service = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    replies: MessageQueue,
    requests: MessageQueue,
    repliers: std.ArrayList(uuid.Uuid),
    replier_index: usize,
    transactions: std.AutoHashMap(uuid.Uuid, Transaction),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .replies = MessageQueue.new(constants.message_queue_max_capacity),
            .requests = MessageQueue.new(constants.message_queue_max_capacity),
            .repliers = std.ArrayList(uuid.Uuid).init(allocator),
            .replier_index = 0,
            .transactions = std.AutoHashMap(uuid.Uuid, Transaction).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.repliers.deinit();
        self.transactions.deinit();
    }

    pub fn tick(self: *Self, connection_messages: *ConnectionMessages) !void {
        // FIX: there are no repliers, this should be an unreachable state as this service should not exist

        // handle aggregating requests to be sent to repliers
        if (self.repliers.items.len > 0) {
            // go over every request and look for a replier
            while (self.requests.dequeue()) |message| {
                const replier = self.getNextReplier();
                _ = replier;
                _ = message;
                _ = connection_messages;

                // if (aggregated_messages.get(replier)) |messages| {
                //     try messages.append(message);
                // } else {
                //     // initialize an array list??
                //
                // }
            }
        } else {
            // TODO: should add topic_name
            log.warn("service has no registered repliers", .{});
        }

        while (self.replies.dequeue()) |message| {
            // match this reply with a transaction
            if (self.transactions.get(message.transactionId())) |transaction| {
                // TODO: message should be added to requestor's list
                _ = transaction;
            } else {
                log.warn("transaction does not exist", .{});
                // message should be destroyed
                message.deref();
                // TODO: should send a message to the replier indicating the reply failed
            }
        }

        // handle aggregating replies to be sent to requestors

    }

    fn getNextReplier(self: *Self) uuid.Uuid {
        self.replier_index = (self.replier_index + 1) % self.repliers.items.len;
        return self.repliers.items[self.replier_index];
    }
};

test "service logic" {
    // const allocator = std.testing.allocator;
    //
    // // aggregated messages that are managed by the broker and live for the lifespan of the `broker.tick`
    // var connection_messages = ConnectionMessages.init(allocator);
    // defer connection_messages.deinit();
    //
    // // services actually don't even know their name?? is this good?
    // var service = Service.init(allocator);
    // defer service.deinit();
    //
    // // add a replier
    // const replier_1_id = uuid.v7.new();
    // const replier_2_id = uuid.v7.new();
    //
    // try service.repliers.append(replier_1_id);
    // try service.repliers.append(replier_2_id);
    //
    // const requestor_id = uuid.v7.new();
    // const transaction_id = uuid.v7.new();
    //
    // var request = Message.new();
    // request.headers.message_type = .request;
    // request.headers.origin_id = requestor_id;
    // request.setTransactionId(transaction_id);
    //
    // // append the message to the replies
    // try service.requests.enqueue(&request);
    //
    // // tick the service. This is called by the broker
    // try service.tick(&connection_messages);
}
