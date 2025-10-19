const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.peer);

const constants = @import("../constants.zig");
const kid = @import("kid");

const stdx = @import("stdx");
const MemoryPool = stdx.MemoryPool;
const RingBuffer = stdx.RingBuffer;
const UnbufferedChannel = stdx.UnbufferedChannel;
const IO = @import("../io.zig").IO;
const LoadBalancer = @import("../load_balancers/load_balancer.zig").LoadBalancer;

const Listener = @import("./listener.zig").Listener;
const ListenerConfig = @import("./listener.zig").ListenerConfig;

const Worker = @import("./worker.zig").Worker;

const Message = @import("../protocol/message.zig").Message;

/// Peer is the central construct for interacting with Kobolds.
pub const Peer = struct {
    const Self = @This();

    pub const Config = struct {
        peer_id: u11 = 0,
        memory_pool_capacity: usize = constants.default_client_memory_pool_capacity,
        inbox_capacity: usize = constants.default_client_inbox_capacity,
        outbox_capacity: usize = constants.default_client_outbox_capacity,
        worker_threads: usize = 1,
        listener_configs: ?[]ListenerConfig = null,
    };

    const State = enum {
        running,
        closing,
        closed,
    };

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    config: Config,
    // connections_mutex: std.Thread.Mutex,
    // connections: std.AutoHashMap(u64, *Connection),
    done_channel: *UnbufferedChannel(bool),
    id: u11,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(*Message),
    io: *IO,
    memory_pool: *MemoryPool(Message),
    // metrics: ClientMetrics,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(*Message),

    listeners: *std.AutoHashMapUnmanaged(usize, *Listener),
    workers: *std.AutoHashMapUnmanaged(usize, *Worker),
    workers_load_balancer: LoadBalancer(usize),
    // services_mutex: std.Thread.Mutex,
    // services: std.StringHashMap(*ClientService),
    // session_mutex: std.Thread.Mutex,
    // session: ?*Session = null,
    state: State,
    // topics_mutex: std.Thread.Mutex,
    // topics: std.StringHashMap(*ClientTopic),
    // transactions_mutex: std.Thread.Mutex,
    // transactions: std.AutoHashMapUnmanaged(u64, *Transaction),

    pub fn init(allocator: std.mem.Allocator, config: Config) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        const memory_pool = try allocator.create(MemoryPool(Message));
        errdefer allocator.destroy(memory_pool);

        memory_pool.* = try MemoryPool(Message).init(allocator, config.memory_pool_capacity);
        errdefer memory_pool.deinit();

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, config.inbox_capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(*Message).init(allocator, config.outbox_capacity);
        errdefer outbox.deinit();

        const workers = try allocator.create(std.AutoHashMapUnmanaged(usize, *Worker));
        errdefer allocator.destroy(workers);

        workers.* = .empty;
        errdefer workers.deinit(allocator);

        const listeners = try allocator.create(std.AutoHashMapUnmanaged(usize, *Listener));
        errdefer allocator.destroy(listeners);

        listeners.* = .empty;
        errdefer listeners.deinit(allocator);

        kid.configure(config.peer_id, .{});

        return Self{
            // .advertiser_callbacks = std.AutoHashMap(u128, AdvertiserCallback).init(allocator),
            .allocator = allocator,
            .close_channel = close_channel,
            .config = config,
            // .connections_mutex = std.Thread.Mutex{},
            // .connections = std.AutoHashMap(u64, *Connection).init(allocator),
            .done_channel = done_channel,
            .id = config.peer_id,
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .outbox_mutex = std.Thread.Mutex{},
            .io = io,
            .memory_pool = memory_pool,
            .listeners = listeners,
            // .metrics = ClientMetrics{},
            // .services_mutex = std.Thread.Mutex{},
            // .services = std.StringHashMap(*ClientService).init(allocator),
            // .session_mutex = std.Thread.Mutex{},
            // .session = null,
            .state = .closed,
            // .topics_mutex = std.Thread.Mutex{},
            // .topics = std.StringHashMap(*ClientTopic).init(allocator),
            // .transactions_mutex = std.Thread.Mutex{},
            // .transactions = .empty,
            .workers = workers,
            .workers_load_balancer = LoadBalancer(usize){
                .round_robin = .init(),
            },
        };
    }

    pub fn deinit(self: *Self) void {
        // var connections_iterator = self.connections.valueIterator();
        // while (connections_iterator.next()) |entry| {
        //     const connection = entry.*;

        //     assert(connection.connection_state == .closed);

        //     connection.deinit();
        //     self.allocator.destroy(connection);
        // }

        // var topics_iterator = self.topics.valueIterator();
        // while (topics_iterator.next()) |entry| {
        //     const topic = entry.*;

        //     self.allocator.free(topic.topic_name);

        //     topic.deinit();
        //     self.allocator.destroy(topic);
        // }

        // var transactions_iterator = self.transactions.valueIterator();
        // while (transactions_iterator.next()) |entry| {
        //     const transaction = entry.*;

        //     self.allocator.destroy(transaction.signal);
        //     self.allocator.destroy(transaction);
        // }

        // var services_iterator = self.services.valueIterator();
        // while (services_iterator.next()) |entry| {
        //     const service = entry.*;

        //     self.allocator.free(service.topic_name);

        //     service.deinit();
        //     self.allocator.destroy(service);
        // }
        var workers_iterator = self.workers.valueIterator();
        while (workers_iterator.next()) |entry| {
            const worker = entry.*;
            worker.deinit();
            self.allocator.destroy(worker);
        }

        var listeners_iterator = self.listeners.valueIterator();
        while (listeners_iterator.next()) |entry| {
            const listener = entry.*;
            listener.deinit();
            self.allocator.destroy(listener);
        }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        while (self.outbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        // if (self.session) |session| {
        //     session.deinit(self.allocator);
        //     self.allocator.destroy(session);
        // }

        // self.connections.deinit();
        // self.transactions.deinit(self.allocator);
        // self.topics.deinit();
        self.inbox.deinit();
        self.io.deinit();
        self.listeners.deinit(self.allocator);
        self.memory_pool.deinit();
        self.outbox.deinit();
        self.workers.deinit(self.allocator);
        switch (self.workers_load_balancer) {
            .round_robin => |*lb| lb.deinit(self.allocator),
        }

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.listeners);
        self.allocator.destroy(self.memory_pool);
        self.allocator.destroy(self.outbox);
        self.allocator.destroy(self.workers);
    }

    pub fn start(self: *Self) !void {
        assert(self.state == .closed);

        // Start the workers
        try self.initializeWorkers();
        try self.spawnWorkerThreads();

        // Start the listeners
        try self.initializeListeners();
        // try self.spawnListeners();

        // // Start the outbound connections
        // try self.initializeOutboundConnections();

        // Start the core thread
        var ready_channel = UnbufferedChannel(bool).new();
        const core_thread = try std.Thread.spawn(.{}, Peer.run, .{ self, &ready_channel });
        core_thread.detach();

        _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
            log.err("core_thread spawn timeout", .{});
            self.close();
            return err;
        };
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                var all_workers_closed = true;
                const safety_limit: usize = 3;
                var i: usize = 0;
                while (i < safety_limit) : (i += 1) {
                    // spin down the workers
                    var worker_iterator = self.workers.valueIterator();
                    while (worker_iterator.next()) |entry| {
                        const worker = entry.*;
                        if (worker.state == .closed) continue;

                        worker.close();
                        all_workers_closed = false;
                    }

                    if (all_workers_closed) break;

                    // reset
                    all_workers_closed = true;
                } else @panic("safety limit breached");

                self.close_channel.send(true);
            },
        }

        _ = self.done_channel.receive();
        log.info("peer {d}: closed", .{self.id});
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        self.state = .running;
        ready_channel.send(true);
        log.info("peer {d} running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("peer {d} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch |err| {
                        log.err("tick failed! {any}", .{err});
                        @panic("core tick failed");
                    };

                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                },
                .closing => {
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                .closed => return,
            }
        }
    }

    fn tick(self: *Self) !void {
        _ = self;
    }

    fn initializeWorkers(self: *Self) !void {
        assert(self.workers.count() == 0);

        for (0..self.config.worker_threads) |id| {
            const worker = try self.allocator.create(Worker);
            errdefer self.allocator.destroy(worker);

            worker.* = try Worker.init(self.allocator, id, self.memory_pool);
            errdefer worker.deinit();

            try self.workers.put(self.allocator, id, worker);
            errdefer _ = self.workers.remove(id);

            switch (self.workers_load_balancer) {
                .round_robin => |*lb| try lb.addItem(self.allocator, id),
            }
        }
    }

    fn spawnWorkerThreads(self: *Self) !void {
        assert(self.workers.count() == self.config.worker_threads);

        var ready_channel = UnbufferedChannel(bool).new();
        // Spawn all of the connection_workers
        var worker_iterator = self.workers.valueIterator();
        while (worker_iterator.next()) |entry| {
            const worker = entry.*;
            assert(worker.state == .closed);

            const worker_thread = try std.Thread.spawn(.{}, Worker.run, .{ worker, &ready_channel });
            worker_thread.detach();

            _ = ready_channel.tryReceive(100 * std.time.ns_per_ms) catch |err| {
                log.err("worker_thread spawn timeout", .{});
                self.close();
                return err;
            };
        }
    }

    fn initializeListeners(self: *Self) !void {
        if (self.config.listener_configs) |listener_configs| {
            // we have already validated that the configuration is valid
            for (listener_configs, 0..listener_configs.len) |listener_config, id| {
                switch (listener_config.transport) {
                    .tcp => {
                        const tcp_listener = try self.allocator.create(Listener);
                        errdefer self.allocator.destroy(tcp_listener);

                        tcp_listener.* = try Listener.init(self.allocator, self.io, id, listener_config);
                        errdefer tcp_listener.deinit();

                        try self.listeners.put(self.allocator, id, tcp_listener);
                    },
                }
            }
        }
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var peer = try Peer.init(allocator, .{});
    defer peer.deinit();

    try peer.start();
    defer peer.close();
}

// Da Rulez:
// 1. in order for a peer to connect to another peer, one of those peers must be listening on a port. That peer
// 2. peer can have `n` connections per session. A session can only be connected to a single peer
// 3. a peer can dynamically listen and unlisten for new connection. Unlistening from connections does not disconnect
//     connections that have already been established through that listening port.
// 4. peers are simple mediums to communicate with eachother. A peer is not a router. A peer is both a client and a
//     server with `n` symetrical connections to another peer.
// 5.
//
// const peer = try Peer.init(allocator, config)
// defer peer.deinit();
//
// spawn a background thread to handle all the io and stuff.
// try peer.start();
// defer peer.close(); // close will destroy all subscriptions, advertisments and all that ungracefully so best to do it deliberatly
//
// // if there was a listener that wasn't spawned w/ the peer_config, then spawn a separate one here. Should
// spawn a new thread per listener
// const listener_id = try peer.listen(listener_config)
// defer peer.unlisten(listener_id);
//
// // create a session and initialize the outbound connection
// // this is a synchronous connect
// const session_id = try peer.connect(outbound_config)
// try peer.awaitConnected(session_id, timeout_ns);
//
// // disconnect all connections associated with this session
// defer peer.disconnect(session_id);
//
// // subscribe to this topic with this session. Sessions can only be connected to a single "peer" so this is pretty safe
// const callback_id = peer.subscribe(session_id, topic_name, callback);
//
// // unsubscribe this function from this session
// defer _ = peer.unsubscribe(session_id, callback_id);
//
//
// try peer.publish(session_id, topic, body, opts)
