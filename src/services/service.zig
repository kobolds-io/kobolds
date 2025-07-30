const std = @import("std");
const constants = @import("../constants.zig");

const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

const Message = @import("../protocol/message.zig").Message;

const Advertiser = @import("./advertiser.zig").Advertiser;
const Transaction = @import("./transaction.zig").Transaction;

pub const ServiceOptions = struct {};

pub const Service = struct {
    const Self = @This();

    advertisers: std.AutoHashMap(u128, *Advertiser),
    allocator: std.mem.Allocator,
    memory_pool: *MemoryPool(Message),
    queue: *RingBuffer(*Message),
    topic_name: []const u8,
    transactions: std.AutoHashMap(u128, Transaction),

    pub fn init(
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        topic_name: []const u8,
    ) !Self {
        const queue = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(queue);

        // TODO: the buffer size should be configured. perhaps this could be a NodeConfig thing
        queue.* = try RingBuffer(*Message).init(allocator, constants.topic_max_queue_capacity);
        errdefer queue.deinit();

        const tmp_copy_buffer = try allocator.alloc(*Message, constants.subscriber_max_queue_capacity);
        errdefer allocator.free(tmp_copy_buffer);

        return Self{
            .advertisers = std.AutoHashMap(u128, *Advertiser).init(allocator),
            .allocator = allocator,
            .memory_pool = memory_pool,
            .queue = queue,
            .topic_name = topic_name,
            .transactions = std.AutoHashMap(u128, Transaction).init(allocator),
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

        self.advertisers.deinit();
        self.transactions.deinit();
    }

    pub fn tick(self: *Self) !void {
        _ = self;

        // I think there should be a single queue, similar to the topic
        // the service will dequeue each message and depending on the message type will do something different.

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
    }

    // FIX: if there are any messages associated with this advertiser, we should see if we can reroute any active
    //     requests OR something better would be to send the requestor an error.
    pub fn removeAdvertiser(self: *Self, advertiser_key: u128) bool {
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
};
