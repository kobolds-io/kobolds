const std = @import("std");
const net = std.net;
const uuid = @import("uuid");

const Connection = @import("./connection.zig").Connection;
const Message = @import("./message.zig").Message;
const Mailbox = @import("./mailbox.zig").Mailbox;

pub const NodeOpts = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
    // token: []const u8, // token needed for nodes to connect to this node
};

pub const ConnectOpts = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4000,
    // token: []const u8, // token needed for nodes to connect to this node
};

pub const Node = struct {
    const Self = @This();
    id: u128,
    host: []const u8,
    port: u16,
    connections: std.AutoHashMap(u128, *Connection) = undefined,
    connections_mutex: std.Thread.Mutex,
    inboxes: std.AutoHashMap(u128, *Mailbox),
    outboxes: std.AutoHashMap(u128, *Mailbox),

    pub fn init(allocator: std.mem.Allocator, opts: NodeOpts) !Node {
        // we should fail if the host port is not  0 >= port <= 65535
        if (opts.host.len == 0) return error.InvalidHost;
        if (opts.port <= 0 or opts.port >= 65535) return error.InvalidPort;

        return Node{
            .id = uuid.v7.new(),
            .host = opts.host,
            .port = opts.port,
            .connections_mutex = std.Thread.Mutex{},
            .connections = std.AutoHashMap(u128, *Connection).init(allocator),
            .inboxes = std.AutoHashMap(u128, *Mailbox).init(allocator),
            .outboxes = std.AutoHashMap(u128, *Mailbox).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // this will remove the references to each of these
        // but we need to still spin down everything that each
        // connection is doing
        var conn_value_iter = self.connections.valueIterator();
        var conn: ?**Connection = conn_value_iter.next();
        while (conn != null) {
            conn.?.*.close();
            conn = conn_value_iter.next();
        }

        self.connections.deinit();

        var inboxes_value_iter = self.inboxes.valueIterator();
        var inbox: ?**Mailbox = inboxes_value_iter.next();
        while (inbox != null) {
            inbox.?.*.deinit();
            inbox = inboxes_value_iter.next();
        }

        self.inboxes.deinit();

        var outboxes_value_iter = self.outboxes.valueIterator();
        var outbox: ?**Mailbox = outboxes_value_iter.next();
        while (outbox != null) {
            outbox.?.*.deinit();
            outbox = outboxes_value_iter.next();
        }

        self.outboxes.deinit();
    }

    pub fn listen(self: *Self) !void {
        const address = try std.net.Address.parseIp(self.host, self.port);

        var listener = try address.listen(.{ .reuse_port = false });
        defer listener.deinit();

        std.debug.print("listening for new node connections on {any}\n", .{address});

        while (listener.accept()) |server_connection| {
            const thread = try std.Thread.spawn(
                .{},
                Node.handleConnection,
                .{ self, server_connection.stream },
            );
            thread.detach();
        } else |err| {
            std.log.err("failed to accept connection {}", .{err});
        }
    }

    pub fn handleConnection(self: *Self, stream: net.Stream) void {
        defer {
            std.debug.print("connections {d}\n", .{self.connections.count()});
        }

        var mailbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const mailbox_allocator = mailbox_gpa.allocator();
        defer _ = mailbox_gpa.deinit();

        const inbox = mailbox_allocator.create(Mailbox) catch |err| {
            std.debug.print("could not create inbox {any}\n", .{err});
            return;
        };
        defer mailbox_allocator.destroy(inbox);

        const outbox = mailbox_allocator.create(Mailbox) catch |err| {
            std.debug.print("could not create outbox {any}\n", .{err});
            return;
        };
        defer mailbox_allocator.destroy(outbox);

        inbox.* = Mailbox.init(mailbox_allocator);
        defer inbox.deinit();

        outbox.* = Mailbox.init(mailbox_allocator);
        defer outbox.deinit();

        var connection = Connection.new(.{ .stream = stream, .inbox = inbox, .outbox = outbox });

        self.inboxes.put(connection.id, inbox) catch |err| {
            std.debug.print("could not add inbox {any}\n", .{err});
            return;
        };
        defer _ = self.inboxes.remove(connection.id);
        self.outboxes.put(connection.id, outbox) catch |err| {
            std.debug.print("could not add outbox {any}\n", .{err});
            return;
        };
        defer _ = self.outboxes.remove(connection.id);

        // TODO: need a mechanic to remove the connection
        self.addConnection(&connection) catch |err| {
            std.debug.print("could not add connection {d} {any}\n", .{ connection.id, err });
            return;
        };
        std.debug.print("connections {d}\n", .{self.connections.count()});
        defer _ = self.removeConnection(connection.id);

        // TODO: create a handle connection function to spawn allocators and all of that

        // spin up the read and write loop
        connection.runSync();

        // -----------------------\\
        // this is an experiment
        // -----------------------\\
        // connection.run();

        // TODO: This should be a channel or an event that blocks until it receives a message
        // while (connection.running) {
        //     std.time.sleep(std.time.ns_per_ms * 100);
        // }
        // -----------------------\\
    }

    /// connect to another node
    pub fn connect(self: *Self, allocator: std.mem.Allocator, opts: ConnectOpts) !*Connection {
        const addr = try std.net.Address.parseIp(opts.host, opts.port);
        const stream = try std.net.tcpConnectToAddress(addr);
        std.debug.print("connected to {s}:{d}\n", .{ opts.host, opts.port });

        var connection = Connection.new(.{ .allocator = allocator, .stream = stream });

        // TODO: need a mechanic to remove the connection
        try self.addConnection(connection);
        errdefer _ = self.removeConnection(connection.id);

        // spawn the thread to run the connection
        const th = try std.Thread.spawn(.{}, Connection.run, .{&connection});
        th.detach();

        return &connection;
    }

    pub fn addConnection(self: *Self, conn: *Connection) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();
        try self.connections.put(conn.id, conn);
    }

    pub fn removeConnection(self: *Self, id: u128) bool {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();
        return self.connections.remove(id);
    }
};

test "the lifecycle" {}

test "it listens for new connections" {}
