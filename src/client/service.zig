const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

// External Dependencies
const uuid = @import("uuid");

// Internal Dependencies
const constants = @import("../constants.zig");

// Protocol
const Message = @import("../protocol/message.zig").Message;
const Request = @import("../protocol/message.zig").Request;
const Reply = @import("../protocol/message.zig").Reply;
const ProtocolError = @import("../errors.zig").ProtocolError;

pub const HandlerFn = *const fn (request: *Message, reply: *Message) void;

pub const Service = struct {
    const Self = @This();

    const Transaction = struct {
        requestor: uuid.Uuid,
        replier: uuid.Uuid,
    };

    allocator: std.mem.Allocator,
    repliers: std.AutoHashMap(uuid.Uuid, HandlerFn),
    requestors: std.ArrayList(uuid.Uuid),
    transactions: std.AutoHashMap(uuid.Uuid, Transaction),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .repliers = std.AutoHashMap(uuid.Uuid, HandlerFn).init(allocator),
            .requestors = std.ArrayList(uuid.Uuid).init(allocator),
            .transactions = std.AutoHashMap(uuid.Uuid, Transaction).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.transactions.deinit();
        self.requestors.deinit();
        self.repliers.deinit();
    }

    pub fn addReplier(self: *Self, replier: uuid.Uuid, handler_fn: HandlerFn) !void {
        if (self.repliers.contains(replier)) {
            return error.ReplierExists;
        } else {
            try self.repliers.put(replier, handler_fn);
        }
    }

    pub fn removeReplier(self: *Self, replier: uuid.Uuid) bool {
        return self.repliers.remove(replier);
    }

    pub fn getNextReplier(self: *Self) uuid.Uuid {
        assert(self.repliers.count() > 0);

        var iter = self.repliers.keyIterator();
        if (iter.next()) |key_ptr| {
            return key_ptr.*;
        } else unreachable;
    }

    pub fn addTransaction(self: *Self, transaction_id: uuid.Uuid, transaction: Transaction) !void {
        if (self.transactions.get(transaction_id)) |_| {
            // this is a duplicate transaction and somthing has gone wrong
            return ProtocolError.DuplicateTransactionId;
        } else {
            try self.transactions.put(transaction_id, transaction);
        }
    }
};

test "service management operations" {
    // const allocator = testing.allocator;
    //
    // const topic_name = "hello";
    // const replier = uuid.v7.new();
    //
    // var sm = ServiceManager.init(allocator);
    // defer sm.deinit();
    //
    // try testing.expectEqual(null, sm.services.get(topic_name));
    //
    // try sm.addReplier(topic_name, replier);
    //
    // const service_opt = sm.services.get(topic_name);
    // try testing.expect(service_opt != null);
    // const service = service_opt.?;
    //
    // try testing.expectEqual(1, service.repliers.items.len);
    //
    // // we now have a service that is ready to accept requests!
    // var req = Message.new();
    // req.headers.message_type = .request;
    // req.headers.origin_id = uuid.v7.new();
    // req.setTopic(topic_name) catch unreachable;
    //
    // // this would be done on the message bus, so there would be no recasting needed
    // var req_headers: *Request = req.headers.into(.request).?;
    // req_headers.transaction_id = uuid.v7.new();
    //
    // const transaction = Service.Transaction{
    //     .requestor = req.headers.origin_id,
    //     .replier = replier,
    // };
    //
    // try testing.expectEqual(0, service.transactions.count());
    //
    // try service.addTransaction(req_headers.transaction_id, transaction);
    //
    // try testing.expectEqual(1, service.transactions.count());
    //
    // // NOTE: later we get a message from the replier
    //
    // // we now have a service that is ready to accept requests!
    // var rep = Message.new();
    // rep.headers.message_type = .reply;
    // rep.headers.origin_id = replier;
    // rep.setTopic(topic_name) catch unreachable;
    //
    // // this would be done on the message bus, so there would be no recasting needed
    // var rep_headers: *Reply = rep.headers.into(.reply).?;
    // rep_headers.transaction_id = req_headers.transaction_id;
    //
    // // find the transaction that we care about
    // const rep_transaction = service.transactions.get(rep_headers.transaction_id).?;
    //
    // // ensure that this replier should be replying to this transaction
    // try testing.expectEqual(rep_transaction.replier, rep_headers.origin_id);
    //
    // // NOTE: at this point, the message bus will forward the reply to the correct requestor
    //
    // // now it is safe to remove the transaction
    // _ = service.transactions.remove(rep_headers.transaction_id);
    //
    // try testing.expectEqual(0, service.transactions.count());
}
