const std = @import("std");
const testing = std.testing;
const log = std.log.scoped(.Worker);
const posix = std.posix;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const uuid = @import("uuid");

const kid = @import("kid");
const stdx = @import("stdx");

const UnbufferedChannel = stdx.UnbufferedChannel;
const RingBuffer = stdx.RingBuffer;
const MemoryPool = stdx.MemoryPool;

const IO = @import("../io.zig").IO;

const Connection = @import("../protocol/connection.zig").Connection;
const InboundConnectionConfig = @import("../protocol/connection.zig").InboundConnectionConfig;
const Envelope = @import("./envelope.zig").Envelope;
const Listener = @import("./listener.zig").Listener;

const Message = @import("../protocol/message.zig").Message;
const ChallengeMethod = @import("../protocol/message.zig").ChallengeMethod;
const ChallengeAlgorithm = @import("../protocol/message.zig").ChallengeAlgorithm;
const TokenEntry = @import("./authenticator.zig").TokenAuthStrategy.TokenEntry;
const Session = @import("./session.zig").Session;

const Handshake = struct {
    nonce: u128,
    connection_id: u64,
    challenge_method: ChallengeMethod,
    algorithm: ChallengeAlgorithm,
};

const WorkerState = enum {
    running,
    closing,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    connections_mutex: std.Thread.Mutex,
    connections: std.AutoHashMapUnmanaged(u64, *Connection),
    listeners_mutex: std.Thread.Mutex,
    listeners: std.AutoHashMapUnmanaged(u64, *Listener),
    inbound_sockets_mutex: std.Thread.Mutex,
    inbound_sockets: std.ArrayList(posix.socket_t),
    // dead_connections_mutex: std.Thread.Mutex,
    // dead_connections: std.array_list.Managed(u128),
    done_channel: *UnbufferedChannel(bool),
    id: usize,
    inbox_mutex: std.Thread.Mutex,
    inbox: *RingBuffer(Envelope),
    io: *IO,
    // node_id: u128,
    memory_pool: *MemoryPool(Message),
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(Envelope),
    state: WorkerState,
    conns_sessions_mutex: std.Thread.Mutex,
    conns_sessions: std.AutoHashMapUnmanaged(u64, u64),
    handshakes: std.AutoHashMapUnmanaged(u64, Handshake),

    pub fn init(
        allocator: std.mem.Allocator,
        id: usize,
        memory_pool: *MemoryPool(Message),
    ) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        const inbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(Envelope).init(allocator, constants.worker_inbox_capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(Envelope));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(Envelope).init(allocator, constants.worker_outbox_capacity);
        errdefer outbox.deinit();

        const io = try allocator.create(IO);
        errdefer allocator.destroy(io);

        io.* = try IO.init(constants.io_uring_entries, 0);
        errdefer io.deinit();

        return Self{
            .allocator = allocator,
            .close_channel = close_channel,
            .connections_mutex = std.Thread.Mutex{},
            .connections = .empty,
            .listeners_mutex = std.Thread.Mutex{},
            .listeners = .empty,
            .inbound_sockets_mutex = std.Thread.Mutex{},
            .inbound_sockets = .empty,
            .handshakes = .empty,
            .conns_sessions_mutex = std.Thread.Mutex{},
            .conns_sessions = .empty,
            .done_channel = done_channel,
            .id = id,
            .inbox = inbox,
            .inbox_mutex = .{},
            .memory_pool = memory_pool,
            .io = io,
            .outbox_mutex = .{},
            .outbox = outbox,
            .state = .closed,
        };
    }

    pub fn deinit(self: *Self) void {
        while (self.inbox.dequeue()) |envelope| {
            envelope.message.deref();
            if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
        }

        while (self.outbox.dequeue()) |envelope| {
            envelope.message.deref();
            if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
        }

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.*;

            conn.deinit();
            self.allocator.destroy(conn);
        }

        var listeners_iter = self.listeners.valueIterator();
        while (listeners_iter.next()) |entry| {
            const listener = entry.*;

            listener.deinit();
            self.allocator.destroy(listener);
        }

        self.inbox.deinit();
        self.outbox.deinit();
        self.io.deinit();
        self.connections.deinit(self.allocator);
        self.listeners.deinit(self.allocator);
        self.inbound_sockets.deinit(self.allocator);
        self.handshakes.deinit(self.allocator);
        self.conns_sessions.deinit(self.allocator);

        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.io);
        self.allocator.destroy(self.outbox);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {d}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.tryReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {d} closing", .{self.id});
                self.state = .closing;
            }

            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    self.io.run_for_ns(constants.io_tick_us * std.time.ns_per_us) catch unreachable;
                },
                .closing => {
                    log.info("worker {d}: closed", .{self.id});
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
                while (!self.closeAllListeners()) {}
                while (!self.closeAllConnections()) {}
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    fn closeAllListeners(self: *Self) bool {
        var all_listeners_closed = true;

        var listeners_iter = self.listeners.valueIterator();
        while (listeners_iter.next()) |entry| {
            var listener = entry.*;
            switch (listener.state) {
                .closed => continue,
                .closing => {
                    all_listeners_closed = false;
                },
                else => {
                    listener.state = .closing;
                    all_listeners_closed = false;
                },
            }
        }

        return all_listeners_closed;
    }

    fn closeAllConnections(self: *Self) bool {
        var all_connections_closed = true;

        var connections_iter = self.connections.valueIterator();
        while (connections_iter.next()) |entry| {
            var conn = entry.*;
            switch (conn.connection_state) {
                .closed => continue,
                .closing => {
                    all_connections_closed = false;
                },
                else => {
                    conn.connection_state = .closing;
                    conn.protocol_state = .terminating;
                    all_connections_closed = false;
                },
            }
        }

        return all_connections_closed;
    }

    pub fn tick(self: *Self) !void {
        try self.tickConnections();
        try self.tickListeners();
        try self.processInboundMessages();
        try self.processOutboundMessages();
    }

    fn tickConnections(self: *Self) !void {
        if (self.connections.count() == 0) return;

        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;

            // log.info("conn_id: {}, conn_state: {any}, protocol_state: {any}", .{
            //     conn.connection_id,
            //     conn.connection_state,
            //     conn.protocol_state,
            // });

            // check if this connection was closed for whatever reason
            if (conn.connection_state == .closed) {
                try self.cleanupConnection(conn);
                continue;
            }

            conn.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };
        }
    }

    fn tickListeners(self: *Self) !void {
        if (self.listeners.count() == 0) return;

        self.listeners_mutex.lock();
        defer self.listeners_mutex.unlock();

        var listeners_iter = self.listeners.valueIterator();
        while (listeners_iter.next()) |entry| {
            const listener = entry.*;

            listener.tick() catch |err| {
                log.err("could not tick connection error: {any}", .{err});
                continue;
            };

            if (listener.sockets.items.len > 0) {
                self.inbound_sockets_mutex.lock();
                defer self.inbound_sockets_mutex.unlock();

                while (listener.sockets.pop()) |socket| {
                    try self.inbound_sockets.append(self.allocator, socket);
                }
            }
        }
    }

    // pub fn processInboundMessages(self: *Self) !void {
    //     self.connections_mutex.lock();
    //     defer self.connections_mutex.unlock();

    //     // var messages_processed: usize = 0;
    //     // defer log.info("worker processed: {} messages", .{messages_processed});

    //     // loop over all connections and gather their messages
    //     var connections_iter = self.connections.iterator();
    //     while (connections_iter.next()) |entry| {
    //         const conn = entry.value_ptr.*;
    //         const session_id_opt = self.conns_sessions.get(conn.connection_id);

    //         while (conn.inbox.dequeue()) |message| {
    //             assert(message.refs() == 1);
    //             // defer messages_processed += 1;
    //             switch (message.fixed_headers.message_type) {
    //                 // .session_init => try self.handleSessionInit(conn, message),
    //                 // .session_join => try self.handleSessionJoin(conn, message),
    //                 else => {
    //                     // NOTE: debugging only ------------------
    //                     // defer {
    //                     //     message.deref();
    //                     //     self.node.memory_pool.destroy(message);
    //                     // }
    //                     // NOTE: debugging only ^^^^^^^^^^^^^^^^^^^^^

    //                     if (message.fixed_headers.message_type == .publish) {
    //                         const received_at = std.time.nanoTimestamp();
    //                         const created_at = std.fmt.parseInt(i128, message.body(), 10) catch 0;

    //                         const diff = @divFloor(received_at - created_at, std.time.ns_per_us);
    //                         log.info("took: {d}us", .{diff});
    //                     }

    //                     if (session_id_opt) |session_id| {
    //                         const envelope = Envelope{
    //                             .message = message,
    //                             .session_id = session_id,
    //                             .conn_id = conn.connection_id,
    //                             .message_id = kid.generate(),
    //                         };

    //                         self.inbox_mutex.lock();
    //                         defer self.inbox_mutex.unlock();

    //                         self.inbox.enqueue(envelope) catch {
    //                             conn.inbox.prepend(message) catch unreachable;
    //                             break;
    //                         };
    //                     } else {
    //                         // message was received but is not associated with a session. Dropping this message
    //                         defer {
    //                             message.deref();
    //                             self.memory_pool.destroy(message);
    //                         }

    //                         log.warn("connection {} is not associated with session", .{conn.connection_id});
    //                     }
    //                 },
    //             }
    //         }
    //     }
    // }
    pub fn processInboundMessages(self: *Self) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        // var messages_processed: usize = 0;
        // defer log.info("worker processed: {} messages", .{messages_processed});

        // loop over all connections and gather their messages
        var connections_iter = self.connections.iterator();
        while (connections_iter.next()) |entry| {
            const conn = entry.value_ptr.*;
            const session_id_opt = self.conns_sessions.get(conn.connection_id);

            while (conn.inbox.dequeue()) |message| {
                assert(message.refs() == 1);

                // defer messages_processed += 1;
                switch (message.fixed_headers.message_type) {
                    .session_init => try self.handleInboundSessionInit(conn, message),
                    .session_join => try self.handleInboundSessionJoin(conn, message),
                    else => {
                        if (session_id_opt) |session_id| {
                            const envelope = Envelope{
                                .message = message,
                                .session_id = session_id,
                                .conn_id = conn.connection_id,
                                .message_id = kid.generate(),
                            };

                            self.inbox_mutex.lock();
                            defer self.inbox_mutex.unlock();

                            self.inbox.enqueue(envelope) catch {
                                conn.inbox.prepend(message) catch unreachable;
                                break;
                            };
                        } else {
                            // message was received but is not associated with a session. Dropping this message
                            defer {
                                message.deref();
                                self.memory_pool.destroy(message);
                            }

                            log.warn("connection {} is not associated with session", .{conn.connection_id});
                        }
                    },
                }
            }
            //             // NOTE: debugging only ------------------
            //             // defer {
            //             //     message.deref();
            //             //     self.node.memory_pool.destroy(message);
            //             // }
            //             // NOTE: debugging only ^^^^^^^^^^^^^^^^^^^^^
            //             // self.inbox_mutex.lock();
            //             // defer self.inbox_mutex.unlock();

            //             // self.inbox.enqueue(envelope) catch {
            //             //     conn.inbox.prepend(message) catch unreachable;
            //             //     break;
            //             // };
            //             // } else {
            //             //     // message was received but is not associated with a session. Dropping this message
            //             //     defer {
            //             //         message.deref();
            //             //         self.memory_pool.destroy(message);
            //             //     }

            //             //     log.warn("connection {} is not associated with session", .{conn.connection_id});
            //             // }
            //         },
            //     }
            // }
        }
    }

    // fn handleSessionJoin2(self: *Self, conn: *Connection, message: *Message) !void {
    //     // we should check if there is already a session

    // }

    fn processOutboundMessages(self: *Self) !void {
        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        while (self.outbox.dequeue()) |envelope| {
            assert(envelope.message.refs() >= 1);
            if (self.connections.get(envelope.conn_id)) |conn| {
                switch (envelope.message.fixed_headers.message_type) {
                    .auth_success => {
                        // log.info("conn_id: {}, env.conn_id: {}, conn.protocol_state: {any}", .{ conn.connection_id, envelope.conn_id, conn.protocol_state });

                        // log.info("auth success!", .{});
                        conn.protocol_state = .ready;
                    },
                    .auth_failure => {
                        // log.info("auth failure!", .{});
                        assert(conn.protocol_state == .authenticating);
                        conn.protocol_state = .terminating;
                    },
                    else => {},
                }

                // log.info("conn.connection_id: {}, envelope.conn_id: {}", .{ conn.connection_id, envelope.conn_id });

                // const end = std.time.nanoTimestamp();
                // const start = std.fmt.parseInt(i128, envelope.message.body(), 10) catch 0;
                // const diff = @divFloor(end - start, std.time.ns_per_us);

                // log.info("took {}us to leave the node", .{diff});
                // log.info("envelope.message {any}", .{envelope.message});

                conn.outbox.enqueue(envelope.message) catch {
                    log.warn("conn outbox full. skipping iteration", .{});
                    self.outbox.prepend(envelope) catch unreachable;
                    break;
                };
            } else {
                log.warn("could not get conn: {}, session: {}", .{ envelope.conn_id, envelope.session_id });
                envelope.message.deref();
                if (envelope.message.refs() == 0) self.memory_pool.destroy(envelope.message);
            }
        }
    }

    pub fn addInboundConnection(self: *Self, conn: *Connection) !void {
        // we are just gonna try to close this socket if anything blows up
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();

        try self.connections.put(self.allocator, conn.connection_id, conn);
        errdefer _ = self.connections.remove(conn.connection_id);

        log.info("worker: {d} added inbound connection {d}", .{ self.id, conn.connection_id });
    }

    fn cleanupConnection(self: *Self, conn: *Connection) !void {
        const conn_id = conn.connection_id;
        defer log.info("worker: {} removed connection {}", .{ self.id, conn_id });

        // if (self.conns_sessions.fetchRemove(conn_id)) |kv_entry| {
        //     const session_id = kv_entry.value;

        //     _ = self.node.removeConnectionFromSession(session_id, conn_id);
        // }

        _ = self.connections.remove(conn_id);

        conn.deinit();
        self.allocator.destroy(conn);
    }

    // fn handlePublish(self: *Self, conn: *Connection, message: *Message) !void {
    //     defer {
    //         message.deref();
    //         if (message.refs() == 0) self.node.memory_pool.destroy(message);
    //     }

    //     if (self.conn_session_map.get(conn.connection_id)) |session_id| {
    //         const envelope = Envelope{
    //             .session_id = session_id,
    //             .message = message,
    //             .conn_id = conn.connection_id,
    //         };

    //         try self.
    //     } else {
    //         log.warn("published message is not associated with a session. dropping message {}", .{
    //             message.extension_headers.publish.message_id,
    //         });
    //     }
    // }

    fn handleInboundSessionInit(self: *Self, conn: *Connection, message: *Message) !void {
        assert(conn.protocol_state == .authenticating);

        const session_id = kid.generate();
        log.info("inbound session_id: {}", .{session_id});

        self.conns_sessions_mutex.lock();
        defer self.conns_sessions_mutex.unlock();

        try self.conns_sessions.put(self.allocator, conn.connection_id, session_id);

        const envelope = Envelope{
            .conn_id = conn.connection_id,
            .message_id = kid.generate(),
            .session_id = session_id,
            .message = message,
        };

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        try self.inbox.enqueue(envelope);
    }

    fn handleInboundSessionJoin(self: *Self, conn: *Connection, message: *Message) !void {
        assert(conn.protocol_state == .authenticating);

        self.conns_sessions_mutex.lock();
        defer self.conns_sessions_mutex.unlock();

        const session_id = message.extension_headers.session_join.session_id;
        try self.conns_sessions.put(self.allocator, conn.connection_id, session_id);

        const envelope = Envelope{
            .conn_id = conn.connection_id,
            .message_id = kid.generate(),
            .session_id = session_id,
            .message = message,
        };

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        try self.inbox.enqueue(envelope);
    }
};

test "init/deinit" {
    const allocator = testing.allocator;

    var memory_pool = try MemoryPool(Message).init(allocator, 10);
    defer memory_pool.deinit();

    var worker = try Worker.init(allocator, 0, &memory_pool);
    defer worker.deinit();
}
