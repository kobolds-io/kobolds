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
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

pub const InboundConnectionConfig = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 0,
    transport: Transport = .tcp,
    peer_type: PeerType = .client,
};

// TODO: a user should be able to provide a token/key for authentication to remotes
pub const OutboundConnectionConfig = struct {
    host: []const u8,
    port: u16,
    transport: Transport = .tcp,
    peer_type: PeerType = .node,
    /// If `null`, no reconnection attempts will be performed.
    reconnect_config: ?ReconnectionConfig = null,
    /// If `null`, no keep alive messages will be performed.
    keep_alive_config: ?KeepAliveConfig = null,
    authentication_config: ?AuthenticationConfig = .{ .none_config = NoneAuthConfig{} },

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

pub const AuthenticationConfig = struct {
    token_config: ?TokenAuthConfig = null,
    none_config: ?NoneAuthConfig = null,
};

pub const TokenAuthConfig = struct {
    token: []const u8,
};

pub const NoneAuthConfig = struct {};

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

pub const PeerType = enum(u8) {
    client,
    node,
};

const ConnectionType = enum {
    inbound,
    outbound,
};

const ConnectionState = enum {
    disconnected,
    connecting,
    connected,
    closing,
    closed,
    err,
};

const ProtocolState = enum {
    inactive,
    accepting,
    authenticating,
    ready,
    terminating,
    terminated,
};

const ConnectionConfig = union(ConnectionType) {
    inbound: InboundConnectionConfig,
    outbound: OutboundConnectionConfig,
};
const ConnectionMetrics = struct {
    bytes_recv_total: u128 = 0,
    bytes_send_total: u128 = 0,
    messages_recv_total: u128 = 0,
    messages_send_total: u128 = 0,
    bytes_recv_at_interval_start: u128 = 0,
    bytes_send_at_interval_start: u128 = 0,
    messages_recv_at_start: u128 = 0,
    messages_send_at_interval_start: u128 = 0,
    interval_start_at: i64 = 0,
    rate_limited: bool = false,
    rate_limited_at: i64 = 0,
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
    connect_submitted: bool,
    inbox: *RingBuffer(*Message),
    io: *IO,
    memory_pool: *MemoryPool(Message),
    messages_recv: u128,
    messages_sent: u128,
    metrics: ConnectionMetrics,
    origin_id: uuid.Uuid,
    outbox: *RingBuffer(*Message),
    parsed_message_ptrs: std.array_list.Managed(*Message),
    parsed_messages: std.array_list.Managed(Message),
    parser: Parser,
    peer_id: uuid.Uuid,
    protocol_state: ProtocolState,
    recv_buffer: []u8,
    recv_bytes: usize,
    recv_completion: *IO.Completion,
    recv_submitted: bool,
    send_buffer_list: *std.array_list.Managed(u8),
    send_buffer_overflow: [constants.message_max_size]u8,
    send_buffer_overflow_count: usize,
    send_completion: *IO.Completion,
    send_submitted: bool,
    socket: posix.socket_t,
    connection_state: ConnectionState,
    tmp_encoding_buffer: []u8,

    pub fn init(
        id: uuid.Uuid,
        origin_id: uuid.Uuid,
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

        const tmp_encoding_buffer = try allocator.alloc(u8, constants.message_max_size);
        errdefer allocator.free(tmp_encoding_buffer);

        const send_buffer_list = try allocator.create(std.array_list.Managed(u8));
        errdefer allocator.destroy(send_buffer_list);

        send_buffer_list.* = try std.array_list.Managed(u8).initCapacity(allocator, constants.connection_send_buffer_size);
        errdefer send_buffer_list.deinit();

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
            .config = config,
            .connect_completion = connect_completion,
            .connection_id = id,
            .connect_submitted = false,
            .inbox = inbox,
            .io = io,
            .memory_pool = memory_pool,
            .messages_recv = 0,
            .messages_sent = 0,
            .metrics = ConnectionMetrics{},
            .origin_id = origin_id,
            .outbox = outbox,
            .parsed_message_ptrs = try std.array_list.Managed(*Message).initCapacity(allocator, constants.connection_recv_buffer_size / @sizeOf(Message)),
            .parsed_messages = try std.array_list.Managed(Message).initCapacity(allocator, constants.connection_recv_buffer_size / @sizeOf(Message)),
            .parser = Parser.init(allocator),
            .peer_id = 0,
            .protocol_state = .inactive,
            .recv_buffer = recv_buffer,
            .recv_bytes = 0,
            .recv_completion = recv_completion,
            .recv_submitted = false,
            .send_buffer_list = send_buffer_list,
            .send_buffer_overflow_count = 0,
            .send_buffer_overflow = undefined,
            .send_completion = send_completion,
            .send_submitted = false,
            .socket = socket,
            .connection_state = .closed,
            .tmp_encoding_buffer = tmp_encoding_buffer,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.connection_state = .closed;
        self.protocol_state = .inactive;

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
        self.send_buffer_list.deinit();

        self.allocator.destroy(self.recv_completion);
        self.allocator.destroy(self.send_completion);
        self.allocator.destroy(self.close_completion);
        self.allocator.destroy(self.connect_completion);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);
        self.allocator.destroy(self.send_buffer_list);

        self.allocator.free(self.recv_buffer);
        self.allocator.free(self.tmp_encoding_buffer);
    }

    pub fn tick(self: *Connection) !void {
        switch (self.connection_state) {
            .closing => {
                // NOTE: uncommenting this line will lead the client to hang when closing connections because
                // self.recv_submitted is never flipped to false
                // if (!self.recv_submitted and !self.send_submitted and !self.connect_submitted) {
                if (self.connection_id == 0) {
                    log.info("uninitialized connection closed {}", .{self.connection_id});
                } else {
                    log.info("connection closed {}", .{self.connection_id});
                }

                self.connection_state = .closed;
                posix.close(self.socket);
                // break out of the tick
                return;
                // }
            },
            .closed => return,
            else => {},
        }

        self.handleRecv();
        self.handleSend();
    }

    fn handleRecv(self: *Self) void {
        if (self.recv_submitted) return;

        self.processInboundMessages();

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

    fn handleSend(self: *Self) void {
        if (self.send_submitted) return;

        self.processOutboundMessages();

        if (self.send_buffer_list.items.len == 0) return;

        self.io.send(
            *Connection,
            self,
            Connection.onSend,
            self.send_completion,
            self.socket,
            self.send_buffer_list.items,
        );

        self.send_submitted = true;
    }

    fn processOutboundMessages(self: *Self) void {
        assert(!self.send_submitted);

        // NOTE: reset the send buffer list. This effectively drops all previous bytes
        self.send_buffer_list.items.len = 0;

        // Handle send buffer overflow first
        if (self.send_buffer_overflow_count > 0) {
            // if there are more bytes in the overflow, then
            if (self.send_buffer_overflow_count > self.send_buffer_list.capacity) {
                // calculate the remaining bytes that can fit into the list
                const remaining_bytes = self.send_buffer_list.capacity - self.send_buffer_list.items.len;

                // append a portion of the remaining bytes
                self.send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..remaining_bytes]);
                self.send_buffer_overflow_count -= remaining_bytes;
            } else {
                // append all of the overflow bytes to the send buffer list
                self.send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..self.send_buffer_overflow_count]);
                self.send_buffer_overflow_count = 0;
            }
        }

        // if there are bytes remaining in the current send_buffer_list and there is a message to send
        if (self.send_buffer_list.capacity - self.send_buffer_list.items.len > 0 and self.outbox.count > 0) {
            var i: usize = 0;
            while (self.outbox.dequeue()) |message| : (i += 1) {
                defer {
                    message.deref();
                    if (message.refs() == 0) self.memory_pool.destroy(message);
                }
                const message_size = message.size();

                message.headers.origin_id = self.origin_id;
                message.headers.connection_id = self.connection_id;

                if (self.tmp_encoding_buffer.len >= message_size) {
                    message.encode(self.tmp_encoding_buffer[0..message_size]);
                } else {
                    log.err("buf len: {}, message_size: {}", .{ self.tmp_encoding_buffer.len, message_size });
                    log.err("message.headers.body_length {any}", .{message.headers.body_length});
                    @panic("buffer was not big enough to hold message");
                }

                // add the maximum number of bytes possible to the send buffer
                const bytes_available: usize = self.send_buffer_list.capacity - self.send_buffer_list.items.len;
                if (bytes_available > self.tmp_encoding_buffer[0..message_size].len) {
                    self.bytes_sent += message_size;
                    self.messages_sent += 1;

                    // append the encoded message to the send_buffer
                    self.send_buffer_list.appendSliceAssumeCapacity(self.tmp_encoding_buffer[0..message_size]);
                } else {
                    self.send_buffer_list.appendSliceAssumeCapacity(self.tmp_encoding_buffer[0..bytes_available]);

                    // save the remaining bytes for the next iteration
                    const remaining_bytes = message_size - bytes_available;

                    @memcpy(
                        self.send_buffer_overflow[0..remaining_bytes],
                        self.tmp_encoding_buffer[bytes_available .. bytes_available + remaining_bytes],
                    );
                    self.send_buffer_overflow_count = remaining_bytes;
                    break;
                }
            }
        }
    }

    fn processInboundMessages(self: *Self) void {
        assert(!self.recv_submitted);

        // self.bytes_recv += bytes;
        self.metrics.bytes_recv_total += self.recv_bytes;
        defer self.recv_bytes = 0;

        // FIX: there should be an assert that enforces that the parser is being passed the right amount
        // of bytes to be parsed
        assert(self.parsed_message_ptrs.items.len == self.parsed_messages.items.len);

        // Parse received bytes into messages
        self.parser.parse(&self.parsed_messages, self.recv_buffer[0..self.recv_bytes]) catch unreachable;

        if (self.parsed_messages.items.len == 0) return;

        self.metrics.messages_recv_total += self.parsed_messages.items.len;

        // Validate messages
        for (self.parsed_messages.items) |message| {
            // assume that invalid messages are poison and close this connection
            if (message.validate()) |reason| {
                self.connection_state = .closing;
                log.err("invalid message: {s}", .{reason});
                return;
            }
        }

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
            message_ptr.* = message;
            message_ptr.ref();

            // NOTE: this is kind of redundent because the memory_pool should be handling this
            assert(message_ptr.refs() == 1);
        }

        // Reset the parsed_messages list
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
            self.connection_state = .closing;
            return;
        }

        self.recv_bytes = bytes;
    }

    pub fn onSend(self: *Connection, comp: *IO.Completion, res: IO.SendError!usize) void {
        _ = comp;
        const bytes_sent: usize = res catch 0;
        self.metrics.bytes_send_total += bytes_sent;

        self.send_submitted = false;
    }

    pub fn onConnect(self: *Connection, _: *IO.Completion, result: IO.ConnectError!void) void {
        self.connect_submitted = false;

        result catch |err| {
            log.err("onConnect err closing conn {any}", .{err});
            self.connection_state = .closing;
            return;
        };

        self.connection_state = .connected;
    }
};
