const std = @import("std");
const net = std.net;
const posix = std.posix;
const assert = std.debug.assert;
const log = std.log.scoped(.Connection);

const uuid = @import("uuid");

const constants = @import("constants.zig");

const Message = @import("./message.zig").Message;
const Parser = @import("./parser.zig").Parser;

// TODO: this should just receive a message allocator and not a specific "message_pool"
const MessagePool = @import("./message_pool.zig").MessagePool;
const MessageQueue = @import("./data_structures/message_queue.zig").MessageQueue;
const RingBuffer = @import("./data_structures/ring_buffer.zig").RingBuffer;
const ProtocolError = @import("./errors.zig").ProtocolError;
const IO = @import("./io.zig").IO;

const ConnectionState = enum {
    ready,
    close,
    closed,
};

pub const Connection = struct {
    allocator: std.mem.Allocator,
    bytes_recv: u128,
    bytes_sent: u128,
    close_completion: *IO.Completion,
    close_submitted: bool,
    inbox: *MessageQueue,
    inbox_mutex: std.Thread.Mutex,
    io: *IO,
    message_pool: *MessagePool,
    messages_recv: u128,
    messages_sent: u128,
    origin_id: uuid.Uuid,
    outbox_mutex: std.Thread.Mutex,
    outbox: *RingBuffer(*Message),
    parser: Parser,
    recv_buffer: []u8,
    recv_completion: *IO.Completion,
    recv_submitted: bool,
    send_buffer: []u8,
    send_completion: *IO.Completion,
    send_submitted: bool,
    socket: posix.socket_t,
    state: ConnectionState,

    pub fn init(
        id: uuid.Uuid,
        io: *IO,
        socket: posix.socket_t,
        inbox: *MessageQueue,
        outbox: *RingBuffer(*Message),
        allocator: std.mem.Allocator,
        message_pool: *MessagePool,
    ) Connection {
        const recv_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(recv_completion);

        const send_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(send_completion);

        const close_completion = allocator.create(IO.Completion) catch unreachable;
        errdefer allocator.destroy(close_completion);

        const recv_buffer = allocator.alloc(u8, constants.connection_recv_buffer_size) catch unreachable;
        errdefer allocator.free(recv_buffer);

        const send_buffer = allocator.alloc(u8, constants.connection_send_buffer_size) catch unreachable;
        errdefer allocator.free(send_buffer);

        return Connection{
            .allocator = allocator,
            .bytes_recv = 0,
            .bytes_sent = 0,
            .close_completion = close_completion,
            .close_submitted = false,
            .inbox = inbox,
            .inbox_mutex = std.Thread.Mutex{},
            .io = io,
            .message_pool = message_pool,
            .messages_recv = 0,
            .messages_sent = 0,
            .origin_id = id,
            .outbox_mutex = std.Thread.Mutex{},
            .outbox = outbox,
            .parser = Parser.init(allocator),
            .recv_buffer = recv_buffer,
            .recv_completion = recv_completion,
            .recv_submitted = false,
            .send_buffer = send_buffer,
            .send_completion = send_completion,
            .send_submitted = false,
            .socket = socket,
            .state = .ready,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.allocator.destroy(self.recv_completion);
        self.allocator.destroy(self.send_completion);
        self.allocator.destroy(self.close_completion);

        self.allocator.free(self.recv_buffer);
        self.allocator.free(self.send_buffer);

        self.parser.deinit();
    }

    pub fn tick(self: *Connection) !void {
        // var timer = try std.time.Timer.start();
        // defer timer.reset();
        // const start = timer.read();
        // defer {
        //     const end = timer.read();
        //     const took = ((end - start) / std.time.ns_per_us);
        //     log.debug("connection tick: {d:6}us", .{took});
        // }

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
                // TODO: Cancel recv submission if there is one

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

        if (!self.send_submitted and self.outbox.count > 0) {
            var fba = std.heap.FixedBufferAllocator.init(self.send_buffer);
            const allocator = fba.allocator();

            var send_buffer_list = try std.ArrayList(u8).initCapacity(allocator, self.send_buffer.len);

            self.outbox_mutex.lock();
            defer self.outbox_mutex.unlock();

            // buffer that will hold any encoded message
            var buf: [constants.message_max_size]u8 = undefined;

            var i: usize = 0;
            while (self.outbox.dequeue()) |message| : (i += 1) {
                const message_size = message.size();
                if (send_buffer_list.items.len + message_size > send_buffer_list.capacity) {
                    // check if adding this message to the send queue would exeed it's capacity
                    // FIX: this message should be added to the head of the queue not the back
                    try self.outbox.enqueue(message);
                    break;
                }

                message.encode(buf[0..message_size]);

                self.bytes_sent += message_size;
                self.messages_sent += 1;

                // append the encoded message to the send_buffer
                send_buffer_list.appendSliceAssumeCapacity(buf[0..message_size]);

                message.deref();
                if (message.ref_count == 0) {
                    self.message_pool.destroy(message);
                }

                // FIX: need a way to ensure this message is destroyed
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
        const bytes = res catch |err| blk: {
            log.err("could not parse bytes {any}", .{err});
            break :blk 0;
        };
        _ = comp;

        // this means that the connection has been closed by the peer and we should
        // shutdown the connection
        if (bytes == 0) {
            self.state = .close;
            return;
        }

        self.bytes_recv += bytes;
        // log.debug("received bytes total: {}, bytes: {}", .{ self.bytes_recv, bytes });

        // create a temporary list that will store messges
        var messages = std.ArrayList(Message).initCapacity(self.allocator, 250) catch unreachable;
        defer messages.deinit();

        // try to parse the bytes into messages
        self.parser.parse(&messages, self.recv_buffer[0..bytes]) catch |err| switch (err) {
            ProtocolError.InvalidHeadersChecksum, ProtocolError.InvalidBodyChecksum => {
                // log.debug("corrupted message found bytes: {any}", .{self.recv_buffer[0..bytes]});
                log.debug("corrupted message found err: {any}", .{err});
                // close this connection
                self.state = .close;
                return;
            },
            else => {
                // this is the case where the message isn't corrupted, we just can't parse it for whatever reason
                log.debug("unable to parse messages {any}", .{err});
            },

            // FIX: we should only accept a limited threshold of errors from the connection
            // if we are receiving many invalid messages, we should simply close the connection.
            // the client could then reconnect and we can get into a healthy state.
        };

        // this includes both valid and invalid messages
        self.messages_recv += messages.items.len;

        self.inbox_mutex.lock();
        defer self.inbox_mutex.unlock();

        // TODO: allocate all messages at the same time instead of serially
        // TODO: enqueue all messages in the inbox simultaneously

        // const ptrs = self.message_pool.createN(@intCast(messages.items.len)) catch unreachable;
        //
        // assert(ptrs.len == messages.items.len);
        //
        // for (ptrs, messages.items) |ptr, message| {
        //     const reason_opt = message.validate();
        //     if (reason_opt) |reason| {
        //         // change the state to .close and exit this block
        //         // on the next iteration of the loop this connection will be closed
        //         self.state = .close;
        //         log.err("invalid message: {s}", .{reason});
        //         return;
        //     }
        //
        //     ptr.* = message;
        //     ptr.*.ref();
        // }
        //
        // self.inbox.enqueueMany(ptrs) catch unreachable;

        for (messages.items, 0..) |message, i| {
            // log.debug("received message {any}", .{message.headers.message_type});

            const reason_opt = message.validate();
            if (reason_opt) |reason| {
                // change the state to .close and exit this block
                // on the next iteration of the loop this connection will be closed
                self.state = .close;
                log.err("invalid message: {s}", .{reason});
                return;
            }

            // NOTE: This is the first time this message enters the system and should immediately be referenced
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

        self.recv_submitted = false;
        // self.recv_buffer = undefined;
    }

    pub fn onRecvTimeout(self: *Connection, comp: *IO.Completion, res: IO.TimeoutError!void) void {
        _ = comp;
        _ = res catch 0;

        self.recv_submitted = false;
        // self.recv_buffer = undefined;

        // std.debug.print("onRecvTimeout!\n", .{});
    }

    pub fn onSend(self: *Connection, comp: *IO.Completion, res: IO.SendError!usize) void {
        _ = comp;
        const bytes_sent: usize = res catch 0;

        // TODO: figure out how to handle these cases
        if (bytes_sent == 0 and self.outbox.count > 0) {
            log.err("was unable to send bytes to connection", .{});
            // TODO: drop all messages for this connection
            log.debug("dereferencing {} messages and clearing outbox", .{self.outbox.count});
            // deref the message so that it can be destroyed later
            while (self.outbox.dequeue()) |message| {
                message.deref();
                if (message.ref_count == 0) {
                    self.message_pool.destroy(message);
                }
            }

            // while (self.outbox.dequeue()) |message| {
            //     message.deref();
            //     if (message.ref_count == 0) {
            //         self.message_pool.destroy(message);
            //     }
            // }

            self.state = .close;
        }

        self.send_submitted = false;
        // self.send_buffer = undefined;
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
