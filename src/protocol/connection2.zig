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
const EventEmitter = @import("stdx").EventEmitter;
const UnbufferedChannel = @import("stdx").UnbufferedChannel;

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
    origin_id: uuid.Uuid,
    remote_id: uuid.Uuid,
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
    // ee: EventEmitter(ConnectionEvent, *anyopaque, ConnectionState)
};

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
