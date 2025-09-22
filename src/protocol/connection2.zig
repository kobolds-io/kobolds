const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Connection);
const testing = std.testing;

const uuid = @import("uuid");
const KID = @import("kid").KID;
const constants = @import("../constants.zig");

const Message = @import("../protocol/message2.zig").Message;
const Parser = @import("../protocol/parser2.zig").Parser;

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
    close_completion: *IO.Completion,
    close_submitted: bool,
    config: ConnectionConfig,
    connect_completion: *IO.Completion,
    connection_id: u64,
    connection_state: ConnectionState,
    connect_submitted: bool,
    inbox: *RingBuffer(*Message),
    io: *IO,
    memory_pool: *MemoryPool(Message),
    messages_buffer: [constants.parser_messages_buffer_size]Message,
    metrics: ConnectionMetrics,
    outbox: *RingBuffer(*Message),
    parser: Parser,
    peer_id: uuid.Uuid,
    protocol_state: ProtocolState,
    recv_buffer: []u8,
    recv_bytes: usize,
    recv_buffer_overflow: [@sizeOf(Message)]u8,
    recv_buffer_overflow_count: usize,
    recv_completion: *IO.Completion,
    recv_submitted: bool,
    send_buffer_list: *std.ArrayList(u8),
    send_buffer_overflow: [@sizeOf(Message)]u8,
    send_buffer_overflow_count: usize,
    send_completion: *IO.Completion,
    send_submitted: bool,
    socket: posix.socket_t,
    tmp_serialization_buffer: []u8,

    pub fn init(
        id: u64,
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

        const tmp_serialization_buffer = try allocator.alloc(u8, @sizeOf(Message));
        errdefer allocator.free(tmp_serialization_buffer);

        const send_buffer_list = try allocator.create(std.ArrayList(u8));
        errdefer allocator.destroy(send_buffer_list);

        send_buffer_list.* = try std.ArrayList(u8).initCapacity(allocator, constants.connection_send_buffer_size);
        errdefer send_buffer_list.deinit(allocator);

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
            .close_completion = close_completion,
            .close_submitted = false,
            .config = config,
            .connect_completion = connect_completion,
            .connection_id = id,
            .connect_submitted = false,
            .inbox = inbox,
            .io = io,
            .memory_pool = memory_pool,
            .metrics = ConnectionMetrics{},
            .outbox = outbox,
            .messages_buffer = undefined,
            .parser = try Parser.init(allocator),
            .peer_id = 0,
            .protocol_state = .inactive,
            .recv_buffer = recv_buffer,
            .recv_bytes = 0,
            .recv_buffer_overflow_count = 0,
            .recv_buffer_overflow = undefined,
            .recv_completion = recv_completion,
            .recv_submitted = false,
            .send_buffer_list = send_buffer_list,
            .send_buffer_overflow_count = 0,
            .send_buffer_overflow = undefined,
            .send_completion = send_completion,
            .send_submitted = false,
            .socket = socket,
            .connection_state = .closed,
            .tmp_serialization_buffer = tmp_serialization_buffer,
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
        self.parser.deinit(self.allocator);
        self.send_buffer_list.deinit(self.allocator);

        self.allocator.destroy(self.recv_completion);
        self.allocator.destroy(self.send_completion);
        self.allocator.destroy(self.close_completion);
        self.allocator.destroy(self.connect_completion);
        self.allocator.destroy(self.inbox);
        self.allocator.destroy(self.outbox);
        self.allocator.destroy(self.send_buffer_list);

        self.allocator.free(self.recv_buffer);
        self.allocator.free(self.tmp_serialization_buffer);
    }

    pub fn tick(self: *Connection) !void {
        switch (self.connection_state) {
            .closing => {
                // NOTE: uncommenting this line will lead the client to hang when closing connections because
                // self.recv_submitted is never flipped to false
                // if (!self.recv_submitted and !self.send_submitted and !self.connect_submitted) {
                if (self.connection_id == 0) {
                    log.info("uninitialized connection closed {d}", .{self.connection_id});
                } else {
                    log.info("connection closed {d}", .{self.connection_id});
                }

                self.connection_state = .closed;
                self.protocol_state = .terminated;
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

        // self.processOutboundMessages2();
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

        // // Handle send buffer overflow first
        // if (self.send_buffer_overflow_count > 0) {
        //     // if there are more bytes in the overflow, then
        //     if (self.send_buffer_overflow_count > self.send_buffer_list.capacity) {
        //         // calculate the remaining bytes that can fit into the list
        //         const remaining_bytes = self.send_buffer_list.capacity - self.send_buffer_list.items.len;

        //         // append a portion of the remaining bytes
        //         self.send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..remaining_bytes]);
        //         self.send_buffer_overflow_count -= remaining_bytes;
        //     } else {
        //         // append all of the overflow bytes to the send buffer list
        //         self.send_buffer_list.appendSliceAssumeCapacity(self.send_buffer_overflow[0..self.send_buffer_overflow_count]);
        //         self.send_buffer_overflow_count = 0;
        //         // self.metrics.messages_send_total += 1;
        //     }

        //     // log.err("handling overflow! {}bytes remain", .{self.send_buffer_overflow_count});
        // }

        // if there are bytes remaining in the current send_buffer_list and there is a message to send
        if (self.send_buffer_list.capacity - self.send_buffer_list.items.len > 0 and self.outbox.count > 0) {
            var i: usize = 0;
            while (self.outbox.dequeue()) |message| : (i += 1) {
                defer {
                    message.deref();
                    if (message.refs() == 0) self.memory_pool.destroy(message);
                }

                assert(self.tmp_serialization_buffer.len > message.packedSize());
                const bytes = message.serialize(self.tmp_serialization_buffer);

                // add the maximum number of bytes possible to the send buffer
                const bytes_available: usize = self.send_buffer_list.capacity - self.send_buffer_list.items.len;
                if (bytes_available > self.tmp_serialization_buffer[0..bytes].len) {
                    self.metrics.bytes_send_total += bytes;
                    self.metrics.messages_send_total += 1;

                    // append the encoded message to the send_buffer
                    self.send_buffer_list.appendSliceAssumeCapacity(self.tmp_serialization_buffer[0..bytes]);
                } else {
                    message.ref();
                    // instead of dealing with the overflow just prepend and try again
                    self.outbox.prepend(message) catch unreachable;
                    break;
                    // self.send_buffer_list.appendSliceAssumeCapacity(self.tmp_serialization_buffer[0..bytes_available]);

                    // // save the remaining bytes for the next iteration
                    // const remaining_bytes = bytes - bytes_available;

                    // @memcpy(
                    //     self.send_buffer_overflow[0..remaining_bytes],
                    //     self.tmp_serialization_buffer[bytes_available .. bytes_available + remaining_bytes],
                    // );
                    // self.send_buffer_overflow_count = remaining_bytes;
                    // break;
                }
            }
        }
    }

    fn processInboundMessages(self: *Self) void {
        assert(!self.recv_submitted);

        self.metrics.bytes_recv_total += self.recv_bytes;
        defer self.recv_bytes = 0;

        // First handle any overflow bytes from last time
        if (self.recv_buffer_overflow_count > 0) {
            const parser_buffer_available_bytes = self.parser.buffer.capacity - self.parser.buffer.items.len;
            if (parser_buffer_available_bytes > 0) {
                const remaining_bytes = @min(parser_buffer_available_bytes, self.recv_buffer_overflow_count);
                self.parser.buffer.appendSliceAssumeCapacity(self.recv_buffer_overflow[0..remaining_bytes]);
                self.recv_buffer_overflow_count -= remaining_bytes;

                if (self.recv_buffer_overflow_count > 0) {
                    // Still not enough space for everything, bail out until next call
                    @panic("i don't think i can do much in this situation :'(");
                }
            }
        }

        var recv_buffer_index: usize = 0;

        // While we still have unread data
        while (recv_buffer_index < self.recv_bytes) {
            const parser_buffer_available_bytes = self.parser.buffer.capacity - self.parser.buffer.items.len;
            if (parser_buffer_available_bytes == 0) {
                // Parser buffer full, stash remaining bytes into overflow
                const remaining_bytes = self.recv_bytes - recv_buffer_index;
                @memcpy(
                    self.recv_buffer_overflow[self.recv_buffer_overflow_count .. self.recv_buffer_overflow_count + remaining_bytes],
                    self.recv_buffer[recv_buffer_index .. recv_buffer_index + remaining_bytes],
                );
                self.recv_buffer_overflow_count += remaining_bytes;
                break;
            }

            const remaining_bytes = @min(self.recv_bytes - recv_buffer_index, parser_buffer_available_bytes);
            self.parser.buffer.appendSliceAssumeCapacity(
                self.recv_buffer[recv_buffer_index .. recv_buffer_index + remaining_bytes],
            );
            recv_buffer_index += remaining_bytes;

            // Now parse until parser can’t produce more messages
            while (true) {
                const parsed_count = self.parser.parse(&self.messages_buffer, &.{}) catch unreachable;
                if (parsed_count == 0) break;

                self.metrics.messages_recv_total += parsed_count;

                const parsed_messages = self.messages_buffer[0..parsed_count];

                for (parsed_messages) |message| {
                    if (message.validate()) |reason| {
                        self.connection_state = .closing;
                        log.err("invalid message: {s}", .{reason});
                        return;
                    }
                }

                const message_ptrs = self.memory_pool.createN(self.allocator, parsed_count) catch |err| {
                    log.err("inbox memory_pool.createN() returned err: {any}", .{err});
                    return;
                };
                defer self.allocator.free(message_ptrs);

                if (message_ptrs.len != parsed_count) {
                    log.err("not enough node ptrs {d} for parsed_messages {d}", .{
                        message_ptrs.len,
                        parsed_count,
                    });
                    for (message_ptrs) |message_ptr| self.memory_pool.destroy(message_ptr);
                    return;
                }

                for (message_ptrs, parsed_messages) |message_ptr, message| {
                    message_ptr.* = message;
                    message_ptr.ref();
                    assert(message_ptr.refs() == 1);
                }

                const messages_enqueued = self.inbox.enqueueMany(message_ptrs);
                if (messages_enqueued < message_ptrs.len) {
                    log.err("could not enqueue all message ptrs. dropping {d} messages", .{message_ptrs[messages_enqueued..].len});
                    for (message_ptrs[messages_enqueued..]) |message_ptr| {
                        message_ptr.deref();
                        self.memory_pool.destroy(message_ptr);
                    }
                }
            }
        }
    }

    // fn processInboundMessages(self: *Self) void {
    //     assert(!self.recv_submitted);

    //     self.metrics.bytes_recv_total += self.recv_bytes;
    //     defer self.recv_bytes = 0;

    //     // handle the case where we have recv_buffer_overflow_bytes
    //     if (self.recv_buffer_overflow_count > 0) {
    //         const parser_buffer_available_bytes = self.parser.buffer.capacity - self.parser.buffer.items.len;
    //         if (self.recv_buffer_overflow_count > parser_buffer_available_bytes) {
    //             if (parser_buffer_available_bytes == 0) unreachable;
    //             // get the maximum number of bytes that can be put into the parser's buffer
    //             const remaining_bytes = @min(parser_buffer_available_bytes, self.recv_buffer_overflow_count);

    //             // add the bytes to the parser.buffer
    //             self.parser.buffer.appendSliceAssumeCapacity(self.recv_buffer_overflow[0..remaining_bytes]);
    //             self.recv_buffer_overflow_count -= remaining_bytes;
    //         } else {
    //             // append all the bytes in the overflow
    //             self.parser.buffer.appendSliceAssumeCapacity(self.recv_buffer_overflow[0..self.recv_buffer_overflow_count]);
    //             self.recv_buffer_overflow_count = 0;
    //         }
    //     }

    //     var recv_buffer_index: usize = 0;

    //     // while we still have bytes left on this read, we need to try to parse
    //     while (recv_buffer_index < self.recv_bytes) {
    //         const parser_buffer_available_bytes = self.parser.buffer.capacity - self.parser.buffer.items.len;
    //         const remaining_bytes = @min(self.recv_bytes - recv_buffer_index, parser_buffer_available_bytes);

    //         if (parser_buffer_available_bytes == 0) {
    //             @memcpy(
    //                 self.recv_buffer_overflow[self.recv_buffer_overflow_count .. self.recv_buffer_overflow_count + remaining_bytes],
    //                 self.recv_buffer[recv_buffer_index .. recv_buffer_index + remaining_bytes],
    //             );

    //             self.recv_buffer_overflow_count += remaining_bytes;
    //             @panic("is this block ever being hit????");
    //         }

    //         const parsed_count = self.parser.parse(
    //             &self.messages_buffer,
    //             self.recv_buffer[recv_buffer_index .. recv_buffer_index + remaining_bytes],
    //         ) catch unreachable;

    //         recv_buffer_index += remaining_bytes;

    //         if (parsed_count == 0) return;

    //         self.metrics.messages_recv_total += parsed_count;

    //         const parsed_messages = self.messages_buffer[0..parsed_count];

    //         // Validate messages
    //         for (parsed_messages) |message| {
    //             if (message.validate()) |reason| {
    //                 self.connection_state = .closing;
    //                 log.err("invalid message: {s}", .{reason});
    //                 return;
    //             }
    //         }

    //         const message_ptrs = self.memory_pool.createN(self.allocator, parsed_count) catch |err| {
    //             log.err("inbox memory_pool.createN() returned err: {any}", .{err});
    //             return;
    //         };
    //         defer self.allocator.free(message_ptrs);

    //         if (message_ptrs.len != parsed_count) {
    //             log.err("not enough node ptrs {d} for parsed_messages {d}", .{
    //                 message_ptrs.len,
    //                 parsed_count,
    //             });
    //             for (message_ptrs) |message_ptr| {
    //                 self.memory_pool.destroy(message_ptr);
    //             }
    //             return;
    //         }

    //         for (message_ptrs, parsed_messages) |message_ptr, message| {
    //             message_ptr.* = message;
    //             message_ptr.ref();

    //             // NOTE: this is kind of redundent because the memory_pool should be handling this
    //             assert(message_ptr.refs() == 1);
    //         }

    //         const n = self.inbox.enqueueMany(message_ptrs);
    //         if (n < message_ptrs.len) {
    //             log.err("could not enqueue all message ptrs. dropping {d} messages", .{message_ptrs[n..].len});
    //             for (message_ptrs[n..]) |message_ptr| {
    //                 message_ptr.deref();
    //                 self.memory_pool.destroy(message_ptr);
    //             }
    //         }
    //     }
    // }

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
            log.err("connection {d} received no bytes, closing", .{self.connection_id});
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

test "init/deinit" {
    const allocator = testing.allocator;

    var kid = KID.init(0, .{});

    var io = try IO.init(16, 0);
    defer io.deinit();

    const socket: posix.socket_t = undefined;

    var memory_pool = try MemoryPool(Message).init(allocator, 10);
    defer memory_pool.deinit();

    const config = ConnectionConfig{ .inbound = .{} };

    var connection = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, config);
    defer connection.deinit();
}

test "processing inbound messages" {
    const allocator = testing.allocator;
    var kid = KID.init(0, .{});

    var io = try IO.init(16, 0);
    defer io.deinit();

    const socket: posix.socket_t = undefined;

    var memory_pool = try MemoryPool(Message).init(allocator, 10_000);
    defer memory_pool.deinit();

    const config = ConnectionConfig{ .inbound = .{} };

    var conn = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, config);
    defer conn.deinit();

    // serailize the message
    var buf: [@sizeOf(Message)]u8 = undefined;

    // fill up the recv buffer with some messages
    var current_index: usize = 0;
    var recv_bytes: usize = 0;
    var messages_count: usize = 0;
    while (true) {
        if (messages_count == conn.inbox.capacity) break;

        var message = Message.new(.publish);
        message.setTopicName("a");
        const bytes = message.serialize(&buf);

        if (bytes > conn.recv_buffer[current_index..].len) break;
        recv_bytes = current_index + bytes;

        // copy the bytes to the recv buffer
        @memcpy(conn.recv_buffer[current_index..recv_bytes], buf[0..bytes]);
        conn.recv_bytes = recv_bytes;
        current_index = recv_bytes;
        messages_count += 1;
    }

    try testing.expectEqual(0, conn.metrics.messages_recv_total);
    try testing.expectEqual(0, conn.metrics.bytes_recv_total);

    // loop over calling process inbound messages until all messages are parsed
    var safety: usize = 0;
    while (conn.metrics.messages_recv_total < messages_count) : (safety += 1) {
        // it should take less that 100 iterations to fully parse all messages
        if (safety > 100) break;
        conn.processInboundMessages();
    }

    try testing.expectEqual(messages_count, conn.metrics.messages_recv_total);
    try testing.expectEqual(recv_bytes, conn.metrics.bytes_recv_total);
}

test "processing outbound messages" {
    const allocator = testing.allocator;
    var kid = KID.init(0, .{});

    var io = try IO.init(16, 0);
    defer io.deinit();

    const socket: posix.socket_t = undefined;

    var memory_pool = try MemoryPool(Message).init(allocator, 10_000);
    defer memory_pool.deinit();

    const config = ConnectionConfig{ .inbound = .{} };

    var conn = try Connection.init(kid.generate(), &io, socket, allocator, &memory_pool, config);
    defer conn.deinit();

    // enqueue a bunch of messages in the outbox
    var messages_count: usize = 0;
    var packed_message_size: usize = 0;
    while (messages_count < conn.outbox.capacity) : (messages_count += 1) {
        const message = try memory_pool.create();
        errdefer memory_pool.destroy(message);

        message.* = Message.new(.publish);
        message.setTopicName("a");
        message.ref();

        packed_message_size = message.packedSize();

        try conn.outbox.enqueue(message);
    }

    try testing.expectEqual(0, conn.metrics.messages_recv_total);
    try testing.expectEqual(0, conn.metrics.bytes_recv_total);

    // loop over calling process inbound messages until all messages are parsed
    var safety: usize = 0;
    while (conn.metrics.messages_send_total < messages_count) : (safety += 1) {
        // it should take less that 100 iterations to fully parse all messages
        if (safety > 3) break;
        conn.processOutboundMessages();
    }

    try testing.expectEqual(messages_count, conn.metrics.messages_send_total);
    try testing.expectEqual(packed_message_size * messages_count, conn.metrics.bytes_send_total);
}
