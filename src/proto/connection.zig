const std = @import("std");
const net = std.net;

const uuid = @import("uuid");
const cbor = @import("zbor");

const utils = @import("./utils.zig");

const Mailbox = @import("./mailbox.zig").Mailbox;
const Message = @import("./message.zig").Message;
const Parser = @import("./parser.zig").Parser;

const ConnectionOpts = struct {
    stream: net.Stream,
    inbox: *Mailbox,
    outbox: *Mailbox,
};

pub const Connection = struct {
    const Self = @This();

    id: uuid.Uuid,
    stream: net.Stream,

    // TODO: this should be atomic
    running: bool = false,
    inbox: *Mailbox = undefined,
    outbox: *Mailbox = undefined,

    pub fn new(opts: ConnectionOpts) Connection {
        return Connection{
            .id = uuid.v7.new(),
            .inbox = opts.inbox,
            .outbox = opts.outbox,
            .stream = opts.stream,
            .running = false,
        };
    }

    pub fn send(self: *Self, message: Message) !void {
        if (!self.running) return error.ConnectionIsNotRunning;

        self.outbox.mutex.lock();
        defer self.outbox.mutex.unlock();

        try self.outbox.append(message);
    }

    // this is slow and inefficient. Should use a channel instead
    // to await for next message w/ timeout. This would help distribute
    // the load across threads
    pub fn receive(self: *Self, timeout_ms: u64) !?Message {
        if (!self.running) return error.ConnectionIsNotRunning;
        if (timeout_ms > std.math.maxInt(u64) - 1_000_000) return error.InvalidTimeout;

        var timer = try std.time.Timer.start();
        defer timer.reset();

        const end: u64 = timer.read() + (timeout_ms * 1_000_000);

        while (self.running) {
            self.inbox.mutex.lock();

            if (self.inbox.messages.items.len > 0) {
                std.log.debug("got message {any}", .{self.inbox.messages.items[0]});
                const v = self.inbox.popHead();
                self.inbox.mutex.unlock();
                return v;
            }

            std.time.sleep(1 * std.time.ns_per_ms);

            // check timeout
            const now = timer.read();
            if (now >= end) {
                self.inbox.mutex.unlock();
                break;
            }
            self.inbox.mutex.unlock();
        }

        return null;
    }

    // TODO: there should be a lock that this uses to block access and hard override the running field
    pub fn close(self: *Self) void {
        if (self.running) {
            self.running = false;
            self.stream.close();
            std.log.debug("connection closed {d}", .{self.id});
        }
    }

    pub fn runSync(self: *Self) void {
        self.running = true;

        const read_loop_thread = std.Thread.spawn(.{}, Connection.readLoop, .{self}) catch |err| {
            std.log.debug("could not spawn readloop {any}", .{err});
            return;
        };

        const write_loop_thread = std.Thread.spawn(.{}, Connection.writeLoop, .{self}) catch |err| {
            std.log.debug("could not spawn writeLoop {any}", .{err});
            return;
        };

        read_loop_thread.join();
        write_loop_thread.join();
    }

    pub fn run(self: *Self) void {
        self.running = true;

        const read_loop_thread = std.Thread.spawn(.{}, Connection.readLoop, .{self}) catch |err| {
            std.log.debug("could not spawn readloop {any}", .{err});
            return;
        };

        const write_loop_thread = std.Thread.spawn(.{}, Connection.writeLoop, .{self}) catch |err| {
            std.log.debug("could not spawn writeLoop {any}", .{err});
            return;
        };

        read_loop_thread.detach();
        write_loop_thread.detach();

        // TODO: Fix this. Instead there should be a channel that each loop publishes to and we can verify that both
        // are running correctly.
        //
        // Temp fix: If we just sleep here for a short time, then any subsequent calls will be ok because the threads
        // would have had time to start up, allocate memory and get going. With this sleep we are introducing addtional
        // time needed for a connection to be established. And so far this is only tested with 2 threads.
        // I'm betting it get's way worse with more connections
        std.time.sleep(1 * std.time.ns_per_ms);
    }

    fn readLoop(self: *Self) void {
        defer {
            self.close();
            std.log.debug("read loop closing", .{});
        }

        var parsed_messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const parsed_messages_allocator = parsed_messages_gpa.allocator();
        defer _ = parsed_messages_gpa.deinit();

        var parsed_messages = std.ArrayList([]u8).init(parsed_messages_allocator);
        defer parsed_messages.deinit();

        // spawn a parser
        var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const parser_allocator = parser_gpa.allocator();
        defer _ = parser_gpa.deinit();

        var parser = Parser.init(parser_allocator);
        defer parser.deinit();

        while (self.running) {
            std.log.debug("read loop", .{});
            var buf: [1024]u8 = undefined;

            const n = self.stream.read(&buf) catch |err| {
                std.log.debug("could not read from stream {any}", .{err});
                return;
            };

            if (n == 0) {
                std.log.debug("read 0 bytes from connection", .{});
                return;
            }

            parser.parse(&parsed_messages, buf[0..n]) catch |err| {
                std.log.debug("could not parse buffer {any}", .{err});
                return;
            };

            std.log.debug("parsed messages [0] {any}", .{parsed_messages.items[0]});

            if (parsed_messages.items.len > 0) {
                // TODO: don't use the page allocator, instead use an already initialized allocator
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                const allocator = arena.allocator();
                defer arena.deinit(); // never deinit?

                // TODO: when we can support multiple encodings, there should be a switch here
                //      to be able to swap between decoding
                for (parsed_messages.items) |msg_bytes| {
                    const di = cbor.DataItem.new(msg_bytes) catch |err| {
                        std.log.debug("could not create cbor DataItem from bytes {any}", .{err});
                        continue;
                    };

                    const message = Message.cborParse(di, .{ .allocator = allocator }) catch |err| {
                        std.log.debug("could not parse message {any}", .{err});
                        continue;
                    };

                    std.log.debug("inbox message {any}", .{message});

                    self.inbox.mutex.lock();
                    defer self.inbox.mutex.unlock();
                    // TODO: there should be a lock here
                    self.inbox.append(message) catch |err| {
                        std.log.debug("could not add message to inbox {any}", .{err});
                        continue;
                    };

                    std.log.debug("inbox.messages.items {any}", .{self.inbox.messages.items});
                }

                parsed_messages.clearAndFree();
            }
        }
    }

    fn writeLoop(self: *Self) void {
        defer {
            self.close();
            std.log.debug("write loop closing", .{});
        }

        const MAX_BYTES: usize = 1024;
        // write buffer
        var write_buffer_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const write_buffer_allocator = write_buffer_arena.allocator();
        defer write_buffer_arena.deinit();

        var write_buffer = std.ArrayList(u8).init(write_buffer_allocator);
        defer write_buffer.deinit();

        while (self.running) {
            // TODO: this should not just do a sleep, instead this should be a channel or something similar
            // that is event based and not poll based. Perhaps a combination of the two
            std.time.sleep(10 * std.time.ns_per_ms);

            self.outbox.mutex.lock();

            // TODO: this should be a function that locks and unlocks the outbox
            if (self.outbox.messages.items.len > 0) {
                // serialize the messages
                for (self.outbox.messages.items) |msg| {
                    utils.serialize(&write_buffer, msg) catch |err| {
                        // TODO: we should remove this message as it is now broken
                        std.log.debug("could not serialize message {any}", .{err});
                        continue;
                    };
                }

                self.outbox.messages.clearAndFree();
            }

            self.outbox.mutex.unlock();

            if (write_buffer.items.len == 0) continue;

            // get the minimum
            const bytes_to_write = if (write_buffer.items.len < MAX_BYTES) write_buffer.items.len else MAX_BYTES;

            const n = self.stream.write(write_buffer.items[0..bytes_to_write]) catch |err| {
                std.log.debug("unable to write to stream {any}", .{err});
                return;
            };

            // shift the remaining bytes to the head of the write_buffer for the next loop
            std.mem.copyForwards(u8, write_buffer.items, write_buffer.items[n..]);
            write_buffer.items.len -= n;
        }
    }
};
