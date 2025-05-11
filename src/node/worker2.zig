const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const posix = std.posix;
const log = std.log.scoped(.Worker);

const uuid = @import("uuid");
const constants = @import("../constants.zig");
const utils = @import("../utils.zig");

const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const IO = @import("../io.zig").IO;
const Node = @import("./node2.zig").Node;

const Connection = @import("../protocol/connection2.zig").Connection;
const Message = @import("../protocol/message.zig").Message;
const Accept = @import("../protocol/message.zig").Accept;

const WorkerState = enum {
    running,
    closing,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    state: WorkerState,
    id: usize,
    node: *Node,
    io: *IO,
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMap(uuid.Uuid, *Connection),

    pub fn init(allocator: std.mem.Allocator, id: usize, node: *Node) !Self {
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

        return Self{
            .id = id,
            .allocator = allocator,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
            .node = node,
            .io = io,
            .connections = std.AutoHashMap(uuid.Uuid, *Connection).init(allocator),
            .connections_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.connections.deinit();
        self.io.deinit();

        self.allocator.destroy(self.io);
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    self.io.run_for_ns(constants.io_tick_ms * std.time.ns_per_ms) catch unreachable;
                },
                .closing => {
                    log.info("worker {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                else => {
                    @panic("unable to tick closed worker");
                },
            }
        }
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                self.connections_mutex.lock();
                defer self.connections_mutex.unlock();

                var connections_iterator = self.connections.valueIterator();
                while (connections_iterator.next()) |entry| {
                    const connection = entry.*;

                    connection.deinit();
                    self.allocator.destroy(connection);
                }
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    pub fn tick(self: *Self) !void {
        _ = self;
    }

    pub fn addConnection(self: *Self, socket: posix.socket_t) !void {
        // we are just gonna try to close this socket if anything blows up
        errdefer posix.close(socket);

        // initialize the connection
        const connection = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(connection);

        const conn_id = uuid.v7.new();
        connection.* = try Connection.init(
            conn_id,
            .node,
            self.io,
            socket,
            self.allocator,
            self.node.memory_pool,
        );
        errdefer connection.deinit();

        connection.state = .connecting;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(conn_id, connection);
        errdefer _ = self.connections.remove(conn_id);

        const accept_message = self.node.memory_pool.create() catch |err| {
            log.err("unable to create an accept message for connection {any}", .{err});
            connection.state = .close;
            return;
        };
        accept_message.* = Message.new();
        accept_message.headers.message_type = .accept;
        accept_message.ref();

        var accept_headers: *Accept = accept_message.headers.into(.accept).?;
        accept_headers.accepted_origin_id = conn_id;
        accept_headers.origin_id = self.node.id;

        connection.state = .connected;

        try connection.outbox.enqueue(accept_message);

        log.info("worker: {} added connection {}", .{ self.id, conn_id });
    }
};
