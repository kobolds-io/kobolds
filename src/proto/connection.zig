const std = @import("std");
const net = std.net;

const uuid = @import("uuid");
const cbor = @import("zbor");

const utils = @import("./utils.zig");

const Mailbox = @import("./mailbox.zig").Mailbox;
const Message = @import("./message.zig").Message;
const Parser = @import("./parser.zig").Parser;

pub const Connection = struct {
    const Self = @This();

    id: u128,
    stream: net.Stream,
    running: bool = false,
    inbox: *Mailbox = undefined,
    outbox: *Mailbox = undefined,

    pub fn new(stream: net.Stream) Connection {
        return Connection{
            .id = uuid.v7.new(),
            .stream = stream,
            .running = false,
        };
    }

    pub fn create(allocator: std.mem.Allocator, stream: net.Stream) !*Connection {
        const conn_ptr = try allocator.create(Connection);
        conn_ptr.* = Connection.new(stream);
        return conn_ptr;
    }

    pub fn deinit(self: *Self) void {
        _ = self;
        // self.write_buffer.deinit();
    }

    pub fn run(self: *Self) void {
        defer {
            std.debug.print("connection closed {d}\n", .{self.id});
            self.running = false;
            self.stream.close();
        }

        var inbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const inbox_allocator = inbox_gpa.allocator();
        defer _ = inbox_gpa.deinit();

        var inbox = Mailbox.init(inbox_allocator);
        defer inbox.deinit();

        var outbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const outbox_allocator = outbox_gpa.allocator();
        defer _ = outbox_gpa.deinit();

        var outbox = Mailbox.init(outbox_allocator);
        defer outbox.deinit();

        self.inbox = &inbox;
        self.outbox = &outbox;

        const read_loop_thread = std.Thread.spawn(.{}, Connection.readLoop, .{self}) catch |err| {
            std.debug.print("could not spawn readloop {any}\n", .{err});
            return;
        };

        const write_loop_thread = std.Thread.spawn(.{}, Connection.writeLoop, .{self}) catch |err| {
            std.debug.print("could not spawn writeLoop {any}\n", .{err});
            return;
        };

        self.running = true;

        read_loop_thread.join();
        write_loop_thread.join();
    }

    fn readLoop(self: *Self) void {
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

        var cbor_parse_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const cbor_parse_allocator = cbor_parse_arena.allocator();
        defer {
            std.debug.print("deiniting the cbor arena\n", .{});
            cbor_parse_arena.deinit(); // never deinit?
        }

        while (true) {
            var buf: [1024]u8 = undefined;
            const n = self.stream.read(&buf) catch |err| {
                std.debug.print("could not read from stream {any}\n", .{err});
                return;
            };

            if (n == 0) {
                std.debug.print("read 0 bytes from connection\n", .{});
                return;
            }

            parser.parse(&parsed_messages, buf[0..n]) catch |err| {
                std.debug.print("could not parse buffer {any}\n", .{err});
                return;
            };

            if (parsed_messages.items.len > 0) {

                // TODO: when we can support multiple encodings, there should be a switch here
                //      to be able to swap between decoding

                for (parsed_messages.items) |msg_bytes| {
                    const di = cbor.DataItem.new(msg_bytes) catch |err| {
                        std.debug.print("could not create cbor DataItem from bytes {any}\n", .{err});
                        continue;
                    };

                    const message = Message.cborParse(di, .{
                        .allocator = cbor_parse_allocator,
                    }) catch |err| {
                        std.debug.print("could not parse message {any}\n", .{err});
                        continue;
                    };

                    self.inbox.mutex.lock();
                    defer self.inbox.mutex.unlock();
                    // TODO: there should be a lock here
                    self.inbox.messages.append(message) catch |err| {
                        std.debug.print("could not add message to inbox {any}\n", .{err});
                        continue;
                    };

                    self.write(message) catch break;

                    std.debug.print("inbox.messages.items {any}\n", .{self.inbox.messages.items});
                }

                parsed_messages.clearAndFree();
            }
        }
    }

    fn writeLoop(self: *Self) void {
        const MAX_BYTES: usize = 1024;
        // write buffer
        var write_buffer_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        const write_buffer_allocator = write_buffer_arena.allocator();
        defer write_buffer_arena.deinit();

        var write_buffer = std.ArrayList(u8).init(write_buffer_allocator);
        defer write_buffer.deinit();

        while (true) {
            // add a small catch here to ensure the function exits
            if (!self.running) return;

            // wait x before trying to see if there are messages ready to be sent
            std.time.sleep(100 * std.time.ns_per_ms);

            // create a new scope here to be able to use defer
            {
                self.outbox.mutex.lock();
                defer self.outbox.mutex.unlock();

                if (self.outbox.messages.items.len > 0) {
                    // serialize the messages
                    for (self.outbox.messages.items) |msg| {
                        std.debug.print("message {any}\n", .{msg});
                        utils.serialize(&write_buffer, msg) catch |err| {
                            // TODO: we should remove this message as it is now broken
                            std.debug.print("could not serialize message {any}\n", .{err});
                            continue;
                        };

                        std.debug.print("outbox message {any}\n", .{msg});
                    }

                    self.outbox.messages.clearAndFree();
                }
            }

            if (write_buffer.items.len == 0) continue;

            // get the minimum
            const bytes_to_write = if (write_buffer.items.len < MAX_BYTES) write_buffer.items.len else MAX_BYTES;

            const n = self.stream.write(write_buffer.items[0..bytes_to_write]) catch |err| {
                std.debug.print("unable to write to stream {any}\n", .{err});
                return;
            };

            // std.debug.print("tried to write: {d} wrote: {d}\n", .{ bytes_to_write, n });
            // std.debug.print("write buffer items {any}\n", .{write_buffer.items});
            // std.debug.print("wrote n bytes {}\n", .{n});

            // shift the remaining bytes to the head of the write_buffer for the next loop
            std.mem.copyForwards(u8, write_buffer.items, write_buffer.items[n..]);
            write_buffer.items.len -= n;
        }
    }

    pub fn write(self: *Self, message: Message) !void {
        if (!self.running) return error.ConnectionIsNotRunning;

        self.outbox.mutex.lock();
        defer self.outbox.mutex.unlock();

        try self.outbox.messages.append(message);
    }
};

test "it deinits correctly" {}
