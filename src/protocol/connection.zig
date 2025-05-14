const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Connection);

const uuid = @import("uuid");

const constants = @import("../constants.zig");

const Message = @import("../protocol/message.zig").Message;
const Parser = @import("../protocol/parser.zig").Parser;
const Accept = @import("../protocol/message.zig").Accept;

const IO = @import("../io.zig").IO;
const ProtocolError = @import("../errors.zig").ProtocolError;

// data structures
const RingBuffer = @import("stdx").RingBuffer;
const MemoryPool = @import("stdx").MemoryPool;

pub const InboundConnectionConfig = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 0,
    transport: Transport = .tcp,
};

// TODO: a user should be able to provide a token/key for authentication to remotes
pub const OutboundConnectionConfig = struct {
    host: []const u8,
    port: u16,
    transport: Transport = .tcp,
    /// The reconnection configuration to be used. If `null`, no reconnection attempts will be performed.
    reconnect_config: ?ReconnectionConfig = null,
    keep_alive_config: ?KeepAliveConfig = null,

    pub fn validate(self: OutboundConnectionConfig) ?[]const u8 {
        if (self.host.len == 0) return "OutboundConnectionConfig `host` invalid length must be > 0";
        if (self.port == 0) return "OutboundConnectionConfig `port` invalid port must be > 0";

        if (self.reconnect_config) |reconnect_config| {
            if (reconnect_config.validate()) |e| return e;
        }
        if (self.keep_alive_config) |keep_alive_config| {
            if (keep_alive_config.validate()) |e| return e;
        }

        return null;
    }
};

pub const KeepAliveConfig = struct {
    enabled: bool = true,
    interval_ms: u64 = 1_000,

    pub fn validate(self: KeepAliveConfig) ?[]const u8 {
        if (!self.enabled) return null;
        if (self.interval_ms < 1) return "KeepAliveConfig `interval` invalid. must be greater than 1ms";

        return null;
    }
};

pub const ReconnectionConfig = struct {
    /// Is this connection allowed be retried
    enabled: bool = true,
    /// The number of attempts to reconnect on connection loss. If `enabled` is true and `max_retries` is 0,
    /// the connection retries will be infinite,
    max_attempts: u32 = 0,
    /// The connection retry strategy to be used for reconnection attempts
    reconnection_strategy: ReconnectionStrategy = .timed,

    pub fn validate(_: ReconnectionConfig) ?[]const u8 {
        return null;
    }
};

pub const ReconnectionStrategy = enum {
    timed,
    exponential_backoff,
};

pub const Transport = enum {
    tcp,
};

const ConnectionConfig = union(ConnectionType) {
    inbound: InboundConnectionConfig,
    outbound: OutboundConnectionConfig,
};

const ConnectionState = enum {
    closing,
    closed,
    connected,
    connecting,
};

const ConnectionType = enum {
    inbound,
    outbound,
};

pub const Connection = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    bytes_recv: u128,
    bytes_sent: u128,
    close_completion: *IO.Completion,
    close_submitted: bool,
    config: ConnectionConfig,
    connect_completion: *IO.Completion,
    connection_id: uuid.Uuid,
    node_id: uuid.Uuid,
    remote_node_id: uuid.Uuid,
    connection_type: ConnectionType,
    connect_submitted: bool,
    inbox: *RingBuffer(*Message),
    io: *IO,
    memory_pool: *MemoryPool(Message),
    messages_recv: u128,
    messages_sent: u128,
    outbox: *RingBuffer(*Message),
    parsed_message_ptrs: std.ArrayList(*Message),
    parsed_messages: std.ArrayList(Message),
    parser: Parser,
    recv_buffer: []u8,
    recv_completion: *IO.Completion,
    recv_submitted: bool,
    send_buffer_overflow: [constants.message_max_size]u8,
    send_buffer_overflow_count: usize,
    send_buffer: []u8,
    send_completion: *IO.Completion,
    send_submitted: bool,
    socket: posix.socket_t,
    state: ConnectionState,

    pub fn init(
        id: uuid.Uuid,
        node_id: uuid.Uuid,
        connection_type: ConnectionType,
        io: *IO,
        socket: posix.socket_t,
        allocator: std.mem.Allocator,
        memory_pool: *MemoryPool(Message),
        config: ConnectionConfig,
    ) !Self {
        const recv_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(recv_completion);

        const send_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(send_completion);

        const close_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(close_completion);

        const connect_completion = try allocator.create(IO.Completion);
        errdefer allocator.destroy(connect_completion);

        const recv_buffer = try allocator.alloc(u8, constants.connection_recv_buffer_size);
        errdefer allocator.free(recv_buffer);

        const send_buffer = try allocator.alloc(u8, constants.connection_send_buffer_size);
        errdefer allocator.free(send_buffer);

        const inbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(inbox);

        inbox.* = try RingBuffer(*Message).init(allocator, constants.connection_inbox_capacity);
        errdefer inbox.deinit();

        const outbox = try allocator.create(RingBuffer(*Message));
        errdefer allocator.destroy(outbox);

        outbox.* = try RingBuffer(*Message).init(allocator, constants.connection_outbox_capacity);
        errdefer outbox.deinit();

        return Self{
            .allocator = allocator,
            .bytes_recv = 0,
            .bytes_sent = 0,
            .close_completion = close_completion,
            .close_submitted = false,
            .connect_completion = connect_completion,
            .connection_type = connection_type,
            .connect_submitted = false,
            .inbox = inbox,
            .io = io,
            .memory_pool = memory_pool,
            .messages_recv = 0,
            .messages_sent = 0,
            .connection_id = id,
            .node_id = node_id,
            .remote_node_id = 0,
            .outbox = outbox,
            .parsed_messages = try std.ArrayList(Message).initCapacity(
                allocator,
                // 50,
                constants.connection_recv_buffer_size / @sizeOf(Message),
            ),
            .parsed_message_ptrs = try std.ArrayList(*Message).initCapacity(
                allocator,
                // 100,
                constants.connection_recv_buffer_size / @sizeOf(Message),
            ),
            .parser = Parser.init(allocator),
            .recv_buffer = recv_buffer,
            .recv_completion = recv_completion,
            .recv_submitted = false,
            .send_buffer_overflow_count = 0,
            .send_buffer_overflow = undefined,
            .send_buffer = send_buffer,
            .send_completion = send_completion,
            .send_submitted = false,
            .socket = socket,
            .state = .closed,
            .config = config,
        };
    }

    pub fn deinit(self: *Connection) void {
        while (self.outbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        while (self.inbox.dequeue()) |message| {
            message.deref();
            if (message.refs() == 0) self.memory_pool.destroy(message);
        }

        self.inbox.deinit();
        self.outbox.deinit();
        self.parsed_messages.deinit();
        self.parsed_message_ptrs.deinit();
        self.parser.deinit();

        self.allocator.destroy(self.recv_completion);
        self.allocator.destroy(self.send_completion);
        self.allocator.destroy(self.close_completion);
        self.allocator.destroy(self.connect_completion);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);

        self.allocator.free(self.recv_buffer);
        self.allocator.free(self.send_buffer);
    }

    pub fn tick(self: *Connection) !void {
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // const start = timer.read();
        // defer {
        //     const end = timer.read();
        //     const took = ((end - start) / std.time.ns_per_us);
        //     log.debug("connection tick: {d:6}us, inbox: {d:6}, outbox: {d:6}", .{
        //         took,
        //         self.inbox.count,
        //         self.outbox.count,
        //     });
        // }

        switch (self.state) {
            .closing => {
                if (self.connection_id == 0) {
                    log.info("uninitialized connection closed {}", .{self.connection_id});
                } else {
                    log.info("connection closed {}", .{self.connection_id});
                }

                self.state = .closed;
                // break out of the tick
                return;
            },
            .closed => return,
            else => {},
        }

        // Submit a recv task to the submission queue
        if (!self.recv_submitted) {
            self.io.recv(
                *Connection,
                self,
                Connection.onRecv,
                self.recv_completion,
                self.socket,
                self.recv_buffer,
            );

            self.recv_submitted = true;
        }

        if (!self.send_submitted) {
            var fba = std.heap.FixedBufferAllocator.init(self.send_buffer);
            const allocator = fba.allocator();

            var send_buffer_list = try std.ArrayList(u8).initCapacity(allocator, self.send_buffer.len);

            if (self.send_buffer_overflow_count > 0) {
                // if there are more bytes in the overflow, then
                if (self.send_buffer_overflow_count > send_buffer_list.capacity) {
                    // calculate the remaining bytes that can fit into the list
                    const remaining_bytes = send_buffer_list.capacity - send_buffer_list.items.len;

                    // append a portion of the remaining bytes
                    send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..remaining_bytes]);
                    self.send_buffer_overflow_count -= remaining_bytes;
                } else {
                    // append all of the overflow bytes to the send buffer list
                    send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..self.send_buffer_overflow_count]);
                    self.send_buffer_overflow_count = 0;
                }
            }

            // if there are bytes remaining in the current send_buffer_list and there is a message to send
            if (send_buffer_list.capacity - send_buffer_list.items.len > 0 and self.outbox.count > 0) {
                // buffer that will hold any encoded message
                var buf: [constants.message_max_size]u8 = undefined;

                var i: usize = 0;
                while (self.outbox.dequeue()) |message| : (i += 1) {
                    defer {
                        message.deref();
                        if (message.refs() == 0) self.memory_pool.destroy(message);
                    }
                    const message_size = message.size();

                    message.headers.node_id = self.node_id;
                    message.headers.connection_id = self.connection_id;

                    message.encode(buf[0..message_size]);

                    // add the maximum number of bytes possible to the send buffer
                    const bytes_available: usize = send_buffer_list.capacity - send_buffer_list.items.len;
                    if (bytes_available > buf[0..message_size].len) {
                        self.bytes_sent += message_size;
                        self.messages_sent += 1;

                        // append the encoded message to the send_buffer
                        send_buffer_list.appendSliceAssumeCapacity(buf[0..message_size]);
                    } else {
                        send_buffer_list.appendSliceAssumeCapacity(buf[0..bytes_available]);

                        // save the remaining bytes for the next iteration
                        const remaining_bytes = message_size - bytes_available;

                        @memcpy(self.send_buffer_overflow[0..remaining_bytes], buf[bytes_available .. bytes_available + remaining_bytes]);
                        self.send_buffer_overflow_count = remaining_bytes;
                        break;
                    }
                }
            }

            self.io.send(
                *Connection,
                self,
                Connection.onSend,
                self.send_completion,
                self.socket,
                send_buffer_list.items,
            );

            self.send_submitted = true;
        }
    }

    pub fn onRecv(self: *Connection, comp: *IO.Completion, res: IO.RecvError!usize) void {
        defer self.recv_submitted = false;

        // Handle receive errors
        const bytes = res catch |err| {
            log.err("could not parse bytes {any}", .{err});
            return;
        };
        _ = comp;

        // Connection closed by peer
        if (bytes == 0) {
            log.err("connection {} received no bytes, closing", .{self.connection_id});
            self.state = .closing;
            return;
        }

        self.bytes_recv += bytes;

        // Parse received bytes into messages
        self.parser.parse(&self.parsed_messages, self.recv_buffer[0..bytes]) catch unreachable;

        if (self.parsed_messages.items.len == 0) return;

        self.messages_recv += self.parsed_messages.items.len;

        // Validate messages
        for (self.parsed_messages.items) |message| {
            // assume that invalid messages are poison and close this connection
            if (message.validate()) |reason| {
                self.state = .closing;
                log.err("invalid message: {s}", .{reason});
                return;
            }
        }

        assert(self.parsed_message_ptrs.items.len >= self.parsed_message_ptrs.items.len);

        const message_ptrs = self.memory_pool.createN(
            self.allocator,
            @intCast(self.parsed_messages.items.len),
        ) catch |err| {
            log.err("inbox memory_pool.createN() returned err: {any}", .{err});
            return;
        };
        defer self.allocator.free(message_ptrs);

        if (message_ptrs.len != self.parsed_messages.items.len) {
            log.err("not enough node ptrs {} for parsed_messages {}", .{ message_ptrs.len, self.parsed_messages.items.len });
            for (message_ptrs) |message_ptr| {
                self.memory_pool.destroy(message_ptr);
            }
            return;
        }

        for (message_ptrs, self.parsed_messages.items) |message_ptr, message| {
            // // Process the handshake message for an accept message
            // if (self.connection_type == .outbound and message.headers.message_type == .accept) {
            //     assert(self.connection_id == 0);

            //     const accept_headers: *const Accept = message.headers.intoConst(.accept).?;

            //     assert(self.remote_node_id != accept_headers.node_id);
            //     assert(self.connection_id != accept_headers.connection_id);

            //     self.connection_id = accept_headers.connection_id;
            //     self.node_id = accept_headers.node_id;

            //     // update the state of this connection to fully connected.
            //     self.state = .connected;
            //     log.info("connection connection_id is set {}", .{self.connection_id});
            // }

            message_ptr.* = message;
            message_ptr.ref();

            // this is kind of redundent because the memory_pool should be handling this
            assert(message_ptr.refs() == 1);
        }

        self.parsed_messages.items.len = 0;

        const n = self.inbox.enqueueMany(message_ptrs);
        if (n < message_ptrs.len) {
            log.err("could not enqueue all message ptrs. dropping {} messages", .{message_ptrs[n..].len});
            for (message_ptrs[n..]) |message_ptr| {
                message_ptr.deref();
                self.memory_pool.destroy(message_ptr);
            }
        }
    }

    pub fn onRecvTimeout(self: *Connection, comp: *IO.Completion, res: IO.TimeoutError!void) void {
        _ = comp;
        _ = res catch 0;

        self.recv_submitted = false;
    }

    pub fn onSend(self: *Connection, comp: *IO.Completion, res: IO.SendError!usize) void {
        _ = comp;
        const bytes_sent: usize = res catch 0;
        _ = bytes_sent;

        self.send_submitted = false;
    }

    pub fn onConnect(self: *Connection, completion: *IO.Completion, result: IO.ConnectError!void) void {
        // reset the submission
        self.connect_submitted = false;

        _ = completion;
        result catch |err| {
            log.err("onConnect err closing conn {any}", .{err});
            self.state = .closing;
        };
    }
};
