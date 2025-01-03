const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Worker);

const uuid = @import("uuid");

const constants = @import("../constants.zig");

const RingBuffer = @import("../data_structures/ring_buffer.zig").RingBuffer;
const MessageQueue = @import("../data_structures/message_queue.zig").MessageQueue;
const Message = @import("../message.zig").Message;
const Accept = @import("../message.zig").Accept;
const Connection = @import("../connection.zig").Connection;
const Bus = @import("./bus.zig").Bus;
const MessagePool = @import("../message_pool.zig").MessagePool;
const IO = @import("../io.zig").IO;

const WorkerState = enum {
    running,
    close,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    node_id: uuid.Uuid,
    allocator: std.mem.Allocator,
    bus: *Bus,
    message_pool: *MessagePool,
    id: u32,
    io: IO,
    state: WorkerState,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),
    connections_mutex: std.Thread.Mutex,
    inbox: MessageQueue,
    inbox_mutex: std.Thread.Mutex,

    pub fn init(
        node_id: uuid.Uuid,
        id: u32,
        allocator: std.mem.Allocator,
        bus: *Bus,
        message_pool: *MessagePool,
    ) !Self {
        var io = try IO.init(8, 0);
        errdefer io.deinit();

        return Self{
            .node_id = node_id,
            .allocator = allocator,
            .id = id,
            .io = io,
            .connections_mutex = std.Thread.Mutex{},
            .state = .closed,
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .inbox = MessageQueue.new(constants.message_queue_capacity_default),
            .inbox_mutex = std.Thread.Mutex{},
            .bus = bus,
            .message_pool = message_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |conn_ptr| {
            var conn = conn_ptr.*;

            // the close function shuold have been called prior to the deinit
            assert(conn.state == .closed);

            // deinit the connection
            conn.deinit();

            // Destroy this connection
            self.allocator.destroy(conn);
        }

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        while (self.inbox.dequeue()) |message| {
            message.deref();
        }

        self.connections.deinit();
        self.io.deinit();
    }

    pub fn tick(self: *Self) !void {
        if (self.connections.count() == 0) return;

        if (self.connections_mutex.tryLock()) {
            defer self.connections_mutex.unlock();
            var conn_iter = self.connections.iterator();
            while (conn_iter.next()) |entry| {
                const conn = entry.value_ptr.*;

                // remove this connection if it is already closed
                if (conn.state == .closed) {
                    conn.deinit();
                    self.allocator.destroy(conn);
                    _ = self.connections.remove(entry.key_ptr.*);
                    continue;
                }

                // send or recv messages
                try conn.tick();

                if (conn.inbox.count > 0) {
                    if (conn.inbox.count + self.inbox.count < self.inbox.capacity) {
                        self.inbox_mutex.lock();
                        defer self.inbox_mutex.unlock();

                        // take all the messages in the inbox
                        self.inbox.concatenate(conn.inbox);

                        // clear the inbox completely
                        conn.inbox.clear();
                    }
                }
            }
        }
    }

    pub fn run(self: *Self) void {
        assert(self.state != .running);

        self.state = .running;
        log.info("worker {} running", .{self.id});
        defer log.debug("worker shutting down {}. state: {any}", .{ self.id, self.state });

        while (true) {
            switch (self.state) {
                .running => {
                    self.tick() catch |err| {
                        log.err("worker.tick err {any}", .{err});
                        unreachable;
                    };
                },
                .close => {
                    self.connections_mutex.lock();
                    defer self.connections_mutex.unlock();

                    // check if every connection is closed
                    var all_connections_closed = true;

                    var conn_iter = self.connections.valueIterator();
                    while (conn_iter.next()) |conn_ptr| {
                        var conn = conn_ptr.*;
                        switch (conn.state) {
                            .closed => continue,
                            .close => {
                                all_connections_closed = false;
                                continue;
                            },
                            .ready => {
                                conn.state = .close;
                                all_connections_closed = false;
                                continue;
                            },
                        }

                        try conn.tick();
                    }

                    if (all_connections_closed) {
                        self.state = .closed;
                        return;
                    }
                },
                .closed => return, // do nothing and exit this loop
            }

            self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
        }
    }

    pub fn close(self: *Self) void {
        assert(self.state == .running);

        self.state = .close;

        // give this worker `n` ms to closed before we are going to just timeout
        const closed_deadline = std.time.milliTimestamp() + 1_000;

        // block until the worker fully closes
        // while (self.state != .closed) {
        while (self.state != .closed and std.time.milliTimestamp() < closed_deadline) {
            std.time.sleep(1 * std.time.ns_per_ms);
            // log.debug("waiting worker.id {}", .{self.id});
        }

        if (self.state == .closed) return;

        log.warn("worker did not properly close all id: {}. deadline exceeded", .{self.id});
    }

    pub fn addConnection(
        self: *Self,
        conn_id: uuid.Uuid,
        socket: posix.socket_t,
        inbox: *MessageQueue,
        outbox: *RingBuffer(*Message),
    ) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        // create the actual connection
        const conn = self.allocator.create(Connection) catch unreachable;
        errdefer self.allocator.destroy(conn);

        conn.* = Connection.init(
            conn_id,
            &self.io,
            socket,
            inbox,
            outbox,
            self.allocator,
            self.message_pool,
        );
        errdefer conn.deinit();

        // add a ref to this connection
        self.connections.put(conn_id, conn) catch unreachable;

        // add an accept message to this connection's inbox immediately
        const accept_message = self.message_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            conn.state = .close;
            return;
        };

        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.node_id;

        log.debug("accept message ref_count {}", .{accept_message.ref_count});

        conn.outbox.enqueue(accept_message) catch unreachable;

        log.info("accepted a connection {}", .{conn.origin_id});
    }
};
