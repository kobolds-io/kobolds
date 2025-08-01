const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.Service);

const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;

const Advertiser = @import("./advertiser.zig").Advertiser;
const Requestor = @import("./requestor.zig").Requestor;
const Transaction = @import("./transaction.zig").Transaction;

const ServiceLoadBalancingStrategy = enum {
    round_robin,
};

const ServiceLoadBalancer = union(ServiceLoadBalancingStrategy) {
    round_robin: RoundRobinLoadBalancer,
};

const RoundRobinLoadBalancer = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    current_index: usize,
    keys: std.ArrayList(u128),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .current_index = 0,
            .keys = std.ArrayList(u128).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.keys.deinit();
    }
};

pub const ServiceOptions = struct {};

pub const Service = struct {
    const Self = @This();

    advertisers: std.AutoHashMap(u128, *Advertiser),
    advertiser_keys: std.ArrayList(u128),
    requestors: std.AutoHashMap(u128, *Requestor),
    allocator: std.mem.Allocator,
    memory_pool: *MemoryPool(Message),
    requests_queue: *RingBuffer(*Message),
    replies_queue: *RingBuffer(*Message),
    topic_name: []const u8,
    transactions: std.AutoHashMap(u128, Transaction),
    load_balancing_strategy: ServiceLoadBalancer,
    tmp_copy_buffer: []*Message,

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        topic_name: []const u8,
    ) !Self {
        const requests_queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(requests_queue);

        requests_queue.* = try RingBuffer(*Message).init(allocator, constants.service_max_requests_queue_capacity);
        errdefer requests_queue.deinit();

        const replies_queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(replies_queue);

        replies_queue.* = try RingBuffer(*Message).init(allocator, constants.service_max_replies_queue_capacity);
        errdefer replies_queue.deinit();

        const tmp_copy_buffer = try allocator.alloc(*Message, constants.advertiser_max_queue_capacity);
        errdefer allocator.free(tmp_copy_buffer);

        return Self{
            .advertisers = std.AutoHashMap(u128, *Advertiser).init(allocator),
            .advertiser_keys = std.ArrayList(u128).init(allocator),
            .requestors = std.AutoHashMap(u128, *Requestor).init(allocator),
            .allocator = allocator,
            .memory_pool = memory_pool,
            .requests_queue = requests_queue,
            .replies_queue = replies_queue,
            .topic_name = topic_name,
            .transactions = std.AutoHashMap(u128, Transaction).init(allocator),
            .load_balancing_strategy = ServiceLoadBalancer{ .round_robin = RoundRobinLoadBalancer.init(allocator) },
            .tmp_copy_buffer = tmp_copy_buffer,
        };
    }

    pub fn deinit(self: *Self) void {
        var advertisers_iter = self.advertisers.valueIterator();
        while (advertisers_iter.next()) |entry| {
            const advertiser = entry.*;

            while (advertiser.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            advertiser.deinit();
            self.allocator.destroy(advertiser);
        }

        var requestors_iter = self.requestors.valueIterator();
        while (requestors_iter.next()) |entry| {
            const requestor = entry.*;

            while (requestor.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            requestor.deinit();
            self.allocator.destroy(requestor);
        }

        switch (self.load_balancing_strategy) {
            .round_robin => self.load_balancing_strategy.round_robin.deinit(),
        }

        self.advertisers.deinit();
        self.advertiser_keys.deinit();
        self.requestors.deinit();
        self.transactions.deinit();
        self.requests_queue.deinit();
        self.replies_queue.deinit();

        self.allocator.destroy(self.replies_queue);
        self.allocator.destroy(self.requests_queue);
        self.allocator.free(self.tmp_copy_buffer);
    }

    pub fn tick(self: *Self) !void {
        if (self.requests_queue.count == 0 and self.replies_queue.count == 0) return;

        try self.handleRequests();
        try self.handleReplies();
        try self.handleTransactions();
        try self.getNextAdvertiserRoundRobin();
    }

    fn handleRequests(self: *Self) !void {
        if (self.requests_queue.count == 0) return;

        // FIX: this is a shit implementation
        if (self.advertisers.count() == 0) {
            while (self.requests_queue.dequeue()) |request| {
                const requestor = self.findOrCreateRequestor(request.headers.connection_id) catch @panic("could not create requestor");
                request.headers.message_type = .reply;
                request.setBody("");
                request.setErrorCode(.err);

                try requestor.queue.enqueue(request);
            }

            return;
        }

        const now = std.time.milliTimestamp();
        while (self.requests_queue.dequeue()) |request| {
            const requestor = self.findOrCreateRequestor(request.headers.connection_id) catch @panic("could not create requestor");
            const advertiser = self.getNextAdvertiser();

            const transaction = Transaction{
                .requestor = requestor,
                .advertiser = advertiser,
                .transaction_id = request.transactionId(),
                .recieved_at = now,
                .timeout = now + 5_000 * std.time.ns_per_ms, // FIX: this should be a timeout provided by the `request`
            };

            try self.transactions.put(transaction.transaction_id, transaction);
            errdefer _ = self.transactions.remove(transaction.transaction_id);

            try advertiser.queue.enqueue(request);
        }
    }

    fn handleReplies(self: *Self) !void {
        if (self.replies_queue.count == 0) return;
        while (self.replies_queue.dequeue()) |reply| {
            if (self.transactions.fetchRemove(reply.transactionId())) |entry| {
                const transaction = entry.value;
                // FIX: there should be better error handling if we are unable to enqueue the reply
                try transaction.requestor.queue.enqueue(reply);
            } else {
                // drop this reply completely
                reply.deref();
                if (reply.refs() == 0) self.memory_pool.destroy(reply);
            }
        }
    }

    fn handleTransactions(self: *Self) !void {
        const now = std.time.milliTimestamp();

        var transactions_iter = self.transactions.valueIterator();
        while (transactions_iter.next()) |entry| {
            const transaction = entry.*;
            const deadline = transaction.recieved_at + transaction.timeout;

            // if this transaction is already timedout, we should try to enqueue a reply for it telling the client
            // that this transaction has timed out. The problem with doing this is that the client will likely
            // have it's own timeout functionality so it may recieve a duplicate timeout.
            if (now >= deadline) {
                const reply = try self.memory_pool.create();
                errdefer self.memory_pool.destroy(reply);

                reply.* = Message.new();
                reply.headers.message_type = .reply;
                reply.setTopicName(self.topic_name);
                reply.setTransactionId(transaction.transaction_id);
                reply.setErrorCode(.timeout);
                reply.ref();
                errdefer reply.deref();

                try transaction.requestor.queue.enqueue(reply);
                _ = self.transactions.remove(transaction.transaction_id);
            }
        }
    }

    fn findOrCreateRequestor(self: *Self, conn_id: u128) !*Requestor {
        const requestor_key = utils.generateKey(self.topic_name, conn_id);

        if (self.requestors.get(requestor_key)) |requestor| {
            return requestor;
        } else {
            const requestor = try self.allocator.create(Requestor);
            errdefer self.allocator.destroy(requestor);

            requestor.* = try Requestor.init(
                self.allocator,
                requestor_key,
                conn_id,
                constants.requestor_max_queue_capacity,
            );
            errdefer requestor.deinit();

            try self.requestors.put(requestor_key, requestor);
            return requestor;
        }
    }

    pub fn addAdvertiser(self: *Self, advertiser_key: u128, conn_id: u128) !void {
        const advertiser = try self.allocator.create(Advertiser);
        errdefer self.allocator.destroy(advertiser);

        advertiser.* = try Advertiser.init(
            self.allocator,
            advertiser_key,
            conn_id,
            constants.advertiser_max_queue_capacity,
        );
        errdefer advertiser.deinit();

        try self.advertisers.put(advertiser_key, advertiser);
        errdefer _ = self.advertisers.remove(advertiser_key);

        switch (self.load_balancing_strategy) {
            .round_robin => |*lb| {
                try lb.keys.append(advertiser_key);
                errdefer _ = lb.keys.pop();

                std.mem.sort(u128, lb.keys.items, {}, std.sort.asc(u128));
            },
        }
    }

    // FIX: if there are any messages associated with this advertiser, we should see if we can reroute any active
    //     requests OR something better would be to send the requestor an error.
    pub fn removeAdvertiser(self: *Self, advertiser_key: u128) bool {
        switch (self.load_balancing_strategy) {
            .round_robin => |*lb| {
                for (lb.keys.items, 0..lb.keys.items.len) |k, index| {
                    if (k == advertiser_key) {
                        // NOTE: I'm like 99% sure that we don't need to resort the list at all if we do an ordered remove
                        //     i'm dumb though
                        _ = lb.keys.orderedRemove(index);
                        break;
                    }
                }
            },
        }

        if (self.advertisers.fetchRemove(advertiser_key)) |entry| {
            const advertiser = entry.value;

            while (advertiser.queue.dequeue()) |message| {
                message.deref();
                if (message.refs() == 0) self.memory_pool.destroy(message);
            }

            advertiser.deinit();
            self.allocator.destroy(advertiser);

            return true;
        }

        return false;
    }

    fn getNextAdvertiser(self: *Self) *Advertiser {
        assert(self.advertisers.count() > 0);

        // switch (self.load_balancing_strategy) {
        //     .round_robin => |*lb| {
        //         if (lb.keys.items.len == 0) {}
        //     },
        // }
        var advertisers_iter = self.advertisers.valueIterator();
        while (advertisers_iter.next()) |entry| {
            const advertiser = entry.*;

            // TODO: figure out a better algorithm for this
            return advertiser;
        }

        unreachable;
    }

    fn getNextAdvertiserRoundRobin(self: *Self) !void {
        assert(self.load_balancing_strategy == .round_robin);
        const lb = self.load_balancing_strategy.round_robin;

        assert(lb.keys.items.len == self.advertisers.count());
        assert(lb.keys.items.len > 0);
        // assert(self.advertiser_keys.items.len == self.advertisers.count());

        // self.load_balancing_strategy.round_robin.current_index = @min(
        //     self.advertiser_keys.items.len - 1,
        //     self.load_balancing_strategy.round_robin.current_index,
        // );
        // defer log.info("load_balancer.current_index {}", .{self.load_balancing_strategy.round_robin.current_index});

        // // const advertiser_key = self.advertiser_keys[self.load_balancing_strategy.round_robin.current_index];

        // // if (self.advertisers.get(advertiser_key)) |advertiser| {
        // //     // try advertiser.queue.enqueue(message);
        // // }

        // // increment the current index so we select the next advertiser
        // self.load_balancing_strategy.round_robin.current_index = (self.load_balancing_strategy.round_robin.current_index + 1) % self.advertiser_keys.items.len;

        // var advertisers_iter = self.advertisers.valueIterator();
        // while (advertisers_iter.next()) |entry| {
        //     const advertiser = entry.*;

        //     // TODO: figure out a better algorithm for this
        //     return advertiser;
        // }
    }
};
