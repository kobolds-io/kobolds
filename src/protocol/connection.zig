const std = @import("std");
const assert = std.debug.assert;
const net = std.net;

const uuid = @import("uuid");

const constants = @import("../constants.zig");
const Parser = @import("./parser.zig").Parser;
const Message = @import("./message.zig").Message;
const Mailbox = @import("../data_structures/mailbox.zig").Mailbox;
const Headers = @import("./message.zig").Headers;
const ProtocolError = @import("./errors.zig").ProtocolError;

// Connection manages, reads and writes to a socket.
pub const Connection = struct {
    const Self = @This();

    id: u128,
    inbox: *Mailbox(Message),
    outbox: *Mailbox(Message),
    stream: net.Stream,
    connected: bool,
    mutex: std.Thread.Mutex,

    sent_count: u32,

    read_loop_running: bool = false,
    write_loop_running: bool = false,

    // should take an allocator, ref to message_bus, socket(stream)
    pub fn new(stream: net.Stream, inbox: *Mailbox(Message), outbox: *Mailbox(Message)) Self {
        return Self{
            .id = uuid.v7.new(),
            .stream = stream,
            .outbox = outbox,
            .inbox = inbox,
            .connected = false,
            .mutex = std.Thread.Mutex{},
            .read_loop_running = false,
            .write_loop_running = false,
            .sent_count = 0,
        };
    }

    pub fn send(self: *Self, message: Message) !void {
        self.outbox.mutex.lock();
        defer self.outbox.mutex.unlock();

        try self.outbox.append(message);
    }

    pub fn close(self: *Self) void {
        if (!self.connected) return;

        self.stream.close();
        self.connected = false;

        var current: u32 = 0;
        const timeout: u32 = 500;
        // wait for both read and write loop threads to spin down
        while (current < timeout) : (current += 1) {
            if (self.write_loop_running or self.read_loop_running) {
                std.time.sleep(1 * std.time.ns_per_ms);
            } else {
                std.log.debug("connection close timeout", .{});
                return;
            }
        }

        std.log.debug("connection closed {d}", .{self.id});
    }

    pub fn run(self: *Self) void {
        assert(!self.connected);

        self.connected = true;
        // self.read_loop_running = true;
        // self.write_loop_running = true;

        defer self.close();
        std.log.debug("running connection {d}", .{self.id});

        const read_loop_thread = std.Thread.spawn(.{}, Connection.readLoop, .{self}) catch unreachable;
        const write_loop_thread = std.Thread.spawn(.{}, Connection.writeLoop, .{self}) catch unreachable;

        read_loop_thread.join();
        write_loop_thread.join();
    }

    fn readLoop(self: *Self) void {
        assert(!self.read_loop_running);
        self.read_loop_running = true;

        defer {
            std.log.debug("connection read loop closing {d}", .{self.id});
            self.read_loop_running = false;
            self.close();
        }

        // var parser_buffer: [constants.parser_max_buffer_size]u8 = undefined;
        // var parser_fba = std.heap.FixedBufferAllocator.init(&parser_buffer);
        var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = parser_gpa.deinit();
        const parser_allocator = parser_gpa.allocator();

        var parser = Parser.init(parser_allocator);
        defer parser.deinit();

        var buf: [constants.connection_recv_buffer_size]u8 = undefined;
        while (self.connected) {
            const n = self.stream.read(&buf) catch |err| {
                std.log.debug("could not read from stream {any}", .{err});
                return;
            };

            if (n == 0) {
                std.log.debug("read 0 bytes from connection, connection was closed by remote", .{});
                return;
            }

            // create a slice to be able to iterate over
            const chunk: []u8 = buf[0..n];

            std.log.debug("read chunk len {d}", .{chunk.len});
            std.log.debug("read chunk {any}", .{chunk});

            var parsed_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
            defer _ = parsed_messages_gpa.deinit();
            const parsed_messages_allocator = parsed_messages_gpa.allocator();

            var messages = std.ArrayList(Message).init(parsed_messages_allocator);
            defer messages.deinit();

            // TODO: Parser should parse `n` messages from the stream, not just one at a time
            parser.parse(&messages, chunk) catch |err| {
                std.log.debug("could not parse messages {any}", .{err});
                @panic("ahhhh");
            };

            std.log.debug("read {d} messages", .{messages.items.len});

            self.inbox.mutex.lock();
            defer self.inbox.mutex.unlock();

            for (messages.items) |message| {
                self.inbox.append(message) catch |err| {
                    std.log.debug("could not append message to inbox {any}", .{err});
                    break;
                };
            }
        }
    }

    fn writeLoop(self: *Self) void {
        assert(!self.write_loop_running);
        self.write_loop_running = true;

        defer {
            std.log.debug("connection write loop closing {d}", .{self.id});
            self.write_loop_running = false;
            self.close();
        }

        // write buffer
        var write_buffer_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const write_buffer_allocator = write_buffer_arena.allocator();
        defer write_buffer_arena.deinit();

        var write_buffer = std.ArrayList(u8).init(write_buffer_allocator);
        defer write_buffer.deinit();

        while (self.connected) {
            std.time.sleep(constants.connection_write_loop_interval);

            // create a scope here so that we can easily lock and unlock the outbox mutex
            {
                self.outbox.mutex.lock();
                defer self.outbox.mutex.unlock();

                while (self.outbox.list.items.len > 0) {
                    const message_opt = self.outbox.popHead();
                    if (message_opt != null) {
                        var message = message_opt.?;

                        var encoded_message_buffer: [constants.message_max_size]u8 = undefined;
                        var encoded_message_fba = std.heap.FixedBufferAllocator.init(&encoded_message_buffer);
                        const encoded_message_allocator = encoded_message_fba.allocator();

                        const encoded_message = encoded_message_allocator.alloc(u8, message.size()) catch unreachable;
                        message.encode(encoded_message);

                        self.sent_count += 1;
                        write_buffer.appendSlice(encoded_message) catch |err| {
                            std.log.debug("could not append encoded message {any}", .{err});
                            return;
                        };
                    }
                }
            }

            if (write_buffer.items.len == 0) continue;

            // get the minimum
            const bytes_to_write = if (write_buffer.items.len < constants.parser_max_buffer_size) write_buffer.items.len else constants.parser_max_buffer_size;

            // FIX: we need to check if we are connected here because there is a
            // possibility that the stream is closed by the time we get here.
            const n = self.stream.write(write_buffer.items[0..bytes_to_write]) catch |err| {
                std.log.debug("unable to write to stream {any}", .{err});
                return;
            };

            // std.debug.print("sent count {d}\n", .{self.sent_count});
            // std.debug.print("wrote {d} bytes\n", .{n});

            // shift the remaining bytes to the head of the write_buffer for the next loop
            std.mem.copyForwards(u8, write_buffer.items, write_buffer.items[n..]);
            write_buffer.items.len -= n;
        }
    }
};
