const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Connection);

const uuid = @import("uuid");

const constants = @import("constants.zig");

const Message = @import("./protocol/message.zig").Message;
const Parser = @import("./protocol/parser.zig").Parser;
const MessagePool = @import("message_pool.zig").MessagePool;
const MessageQueue = @import("./data_structures//message_queue.zig").MessageQueue;
const ProtocolError = @import("./protocol/errors.zig").ProtocolError;
const IO = @import("./io.zig").IO;

const ConnectionState = enum {
    ready,
    close,
    closed,
};

pub const Connection = struct {
    id: uuid.Uuid,
    socket: posix.socket_t,
    message_pool: *MessagePool,
    io: *IO,

    state: ConnectionState,

    recv_submitted: bool,
    recv_completion: *IO.Completion,
    recv_buffer: [constants.connection_recv_buffer_size]u8,

    send_submitted: bool,
    send_completion: *IO.Completion,
    send_buffer: [constants.connection_send_buffer_size]u8,

    close_submitted: bool,
    close_completion: *IO.Completion,

    parser: Parser,

    allocator: std.mem.Allocator,

    inbox: MessageQueue,
    outbox: MessageQueue,

    inbox_mutex: std.Thread.Mutex,
    outbox_mutex: std.Thread.Mutex,

    pub fn init(
        message_pool: *MessagePool,
        io: *IO,
        socket: posix.socket_t,
        allocator: std.mem.Allocator,
    ) Connection {
        const recv_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(recv_completion);

        const send_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(send_completion);

        const close_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(close_completion);

        return Connection{
            .id = 0,
            .message_pool = message_pool,
            .io = io,
            .socket = socket,
            .state = .ready,
            .recv_submitted = false,
            .recv_completion = recv_completion,
            .recv_buffer = undefined,
            .send_submitted = false,
            .send_completion = send_completion,
            .send_buffer = undefined,
            .close_submitted = false,
            .close_completion = close_completion,
            .parser = Parser.init(allocator),
            .allocator = allocator,
            .inbox = MessageQueue.new(null),
            .outbox = MessageQueue.new(null),
            .inbox_mutex = std.Thread.Mutex{},
            .outbox_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Connection) void {
        self.allocator.destroy(self.recv_completion);
        self.allocator.destroy(self.send_completion);
        self.allocator.destroy(self.close_completion);

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        self.outbox_mutex.lock();
        defer self.outbox_mutex.unlock();

        while (self.inbox.dequeue()) |message_ptr| {
            message_ptr.deref();
            self.message_pool.destroy(message_ptr);
        }

        while (self.outbox.dequeue()) |message| {
            message.deref();
            self.message_pool.destroy(message);
        }

        // log.debug("remaining messages in message_pool {d}", .{self.message_pool.unassigned_queue.count});

        self.parser.deinit();
    }

    pub fn tick(self: *Connection) !void {
        switch (self.state) {
            .close => {
                if (!self.close_submitted) {
                    self.io.close(
                        *Connection,
                        self,
                        Connection.onClose,
                        self.close_completion,
                        self.socket,
                    );
                }

                // TODO: Cancel send submission if there is one
                // TODO: Cancel read submission if there is one

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
                &self.recv_buffer,
            );

            self.recv_submitted = true;
        }

        if (!self.send_submitted and !self.outbox.isEmpty()) {
            var fba = std.heap.FixedBufferAllocator.init(&self.send_buffer);
            const allocator = fba.allocator();

            var send_buffer_list = try std.ArrayList(u8).initCapacity(allocator, self.send_buffer.len);

            self.outbox_mutex.lock();
            defer self.outbox_mutex.unlock();

            // buffer that will hold any encoded message
            var buf: [constants.message_max_size]u8 = undefined;

            while (self.outbox.dequeue()) |message| {
                assert(message.ref_count >= 1);
                const message_size = message.size();
                if (send_buffer_list.items.len + message_size > send_buffer_list.capacity) {
                    // we should push this message back to the head of the queue and break this loop;
                    self.outbox.prepend(message) catch unreachable;
                    break;
                }

                message.encode(buf[0..message_size]);

                // append the encoded message to the send_buffer
                send_buffer_list.appendSliceAssumeCapacity(buf[0..message_size]);

                message.deref();

                if (message.ref_count == 0) {
                    self.message_pool.destroy(message);
                } else {
                    log.debug("dangling connection.outbox message! type: {any} ref: {any}", .{ message.headers.message_type, message.ref_count });
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

        // log.debug("messages available {}", .{self.message_pool.unassigned_queue.count});
    }

    pub fn onRecv(self: *Connection, comp: *IO.Completion, res: IO.RecvError!usize) void {
        const bytes = res catch return;
        _ = comp;

        // this means that the connection has been closed by the peer and we should
        // shutdown the connection
        if (bytes == 0) {
            self.state = .close;
            return;
        }

        // log.debug("recv bytes {}", .{bytes});

        // create a temporary list that will store messges
        var messages = std.ArrayList(Message).initCapacity(self.allocator, 10) catch unreachable;
        defer messages.deinit();

        // try to parse the bytes into messages
        self.parser.parse(&messages, self.recv_buffer[0..bytes]) catch |err| switch (err) {
            ProtocolError.InvalidHeadersChecksum, ProtocolError.InvalidBodyChecksum => {
                // log.debug("corrupted message found bytes: {any}", .{self.recv_buffer[0..bytes]});
                // log.debug("corrupted message found err: {any}", .{err});
                // close this connection
                self.state = .close;
                return;
            },
            else => {
                // this is the case where the message isn't corrupted, we just can't parse it
                log.debug("unable to parse messages {any}", .{err});
            },

            // FIX: we should only accept a limited threshold of errors from the connection
            // if we are receiving many invalid messages, we should simply close the connection.
            // the client could then reconnect and we can get into a healthy state.
        };

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        for (messages.items, 0..) |message, i| {
            const reason_opt = message.validate();
            if (reason_opt) |reason| {
                // change the state to .close and exit this block
                // on the next iteration of the loop this connection will be closed
                self.state = .close;
                log.err("invalid message: {s}", .{reason});
                return;
            }

            // NOTE: This is the first time this message enters the system and should
            // immediately be referenced
            const message_ptr = self.message_pool.create() catch |err| {
                log.err("inbox message_pool.create() returned err: {any}", .{err});
                log.err("dropping {d} messages", .{messages.items.len - i});
                return;
            };

            message_ptr.* = message;
            message_ptr.ref();

            // ensure that the message isn't previously allocated to some other message
            assert(message_ptr.ref_count == 1);

            // TODO: it would be faster to enqueue many message pointers at once
            self.inbox.enqueue(message_ptr) catch |err| {
                log.err("self.inbox.count {any}", .{self.inbox.count});
                log.err("could not enqueue message ptr: {*}, err: {any}", .{ message_ptr, err });
                log.err("dropping {d} messages", .{messages.items.len});

                message_ptr.deref();
                self.message_pool.destroy(message_ptr);

                return;
            };
        }

        // log.debug("received {} messages", .{messages.items.len});

        self.recv_submitted = false;
        self.recv_buffer = undefined;
    }

    pub fn onRecvTimeout(self: *Connection, comp: *IO.Completion, res: IO.TimeoutError!void) void {
        _ = comp;
        _ = res catch 0;

        self.recv_submitted = false;
        self.recv_buffer = undefined;

        // std.debug.print("onRecvTimeout!\n", .{});
    }

    pub fn onSend(self: *Connection, comp: *IO.Completion, res: IO.SendError!usize) void {
        _ = comp;
        const bytes_sent: usize = res catch 0;

        // TODO: figure out how to handle these cases
        if (bytes_sent == 0) unreachable;

        self.send_submitted = false;
        self.send_buffer = undefined;

        // log.debug("send bytes {}", .{bytes_sent});
    }

    pub fn onClose(self: *Connection, comp: *IO.Completion, res: IO.CloseError!void) void {
        res catch {};
        _ = comp;

        // this means that the connection has been closed by the peer and we should
        // shutdown the connection
        self.state = .closed;
        self.close_submitted = false;

        std.debug.print("closed connection\n", .{});
    }
};
