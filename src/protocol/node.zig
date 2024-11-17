const std = @import("std");
const net = std.net;
const assert = std.debug.assert;

const uuid = @import("uuid");

const Connection = @import("connection.zig").Connection;
const Message = @import("message.zig").Message;
const Mailbox = @import("mailbox.zig").Mailbox;
const MessageType = @import("message.zig").MessageType;
const ProtocolError = @import("./errors.zig").ProtocolError;

// on message: *const fn (message_bus: *Self, message: *Message) void

/// static configuration used to configure the node
pub const NodeConfig = struct {
    /// the host the node is bound to
    host: []const u8 = "127.0.0.1",
    /// tcp connections are accepted on this port
    port: u16 = 4000,
};

/// A node is the central building block for communicating between nodes
/// on either the same or different hosts.
pub const Node = struct {
    const Self = @This();

    id: u128,
    listening: bool,

    /// configuration file used by the node
    config: NodeConfig,
    /// map of all current connetions
    connections: std.AutoHashMap(u128, *Connection) = undefined,
    /// mutex used to control access to the connections map
    connections_mutex: std.Thread.Mutex,
    /// allocator used for connections
    allocator: std.mem.Allocator,
    /// global inbox used to handle incoming messages
    inbox: Mailbox(Message),

    /// initialize the node so it is ready to work
    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) Self {
        return Self{
            .id = uuid.v7.new(),
            .config = config,
            .connections_mutex = std.Thread.Mutex{},
            .allocator = allocator,
            .inbox = Mailbox(Message).init(allocator),
            .connections = undefined,
            .listening = false,
        };
    }

    /// deinitialize the node and drop all references
    pub fn deinit(self: *Self) void {
        if (self.listening) {
            self.connections.deinit(); // should lock?
            self.listening = false;
        }
    }

    /// applies the NodeConfig and runs the node. this is a blocking function
    /// and should be used via the CLI
    pub fn run(self: *Self) !void {
        assert(!self.listening);
        assert(self.connections.count() == 0);

        // initialize the connections bus
        var connections_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = connections_gpa.deinit();
        const connections_allocator = connections_gpa.allocator();

        self.connections = std.AutoHashMap(u128, *Connection).init(connections_allocator);
        defer self.connections.deinit();

        // TODO: handle connecting to other nodes

        // handle listening for new node connections.
        return try self.listen();
    }

    /// listen for incomming connections from nodes or clients
    fn listen(self: *Self) !void {
        // ensure that the node is not already listening.
        assert(!self.listening);

        const addr = try std.net.Address.parseIp(self.config.host, self.config.port);

        // don't allow reuse of a port so that nodes can remain independent.
        var listener = try addr.listen(.{ .reuse_port = false });
        defer listener.deinit();

        self.listening = true;
        defer {
            self.listening = false;
        }

        std.log.debug("listening for new connections on {any}", .{addr});

        while (listener.accept()) |server_connection| {
            const stream = server_connection.stream;

            // TODO: this should instead use a better strategy like using io_uring
            // instead of a single thread per connection. for now this is fine.
            const thread = std.Thread.spawn(.{}, Node.handleConnection, .{ self, stream }) catch unreachable;
            // don't wait for this to return
            thread.detach();
        } else |err| {
            std.log.err("failed to accept connection {}", .{err});
        }
    }

    /// handler of new connections.
    fn handleConnection(self: *Self, stream: net.Stream) void {
        // add the connection to the connection map
        defer {
            std.log.debug("connections {d}", .{self.connections.count()});
        }

        var connection_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const connection_allocator = connection_gpa.allocator();
        defer _ = connection_gpa.deinit();

        // create a connection w/ all the appropriate allocators
        var outbox_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const outbox_allocator = outbox_gpa.allocator();
        defer _ = outbox_gpa.deinit();

        // create an outbox that will handle all messages going outbound from this connection
        var outbox = Mailbox(Message).init(outbox_allocator);
        defer outbox.deinit();

        // the allocator that we use here should come from the node, not a "fresh" allocator just inside this func
        const connection = connection_allocator.create(Connection) catch |err| {
            std.log.debug("could not allocate connection {any}", .{err});
            return;
        };
        defer connection_allocator.destroy(connection);

        connection.* = Connection.new(stream, &self.inbox, &outbox);

        // TODO: need a mechanic to remove the connection
        self.addConnection(connection) catch |err| {
            std.log.debug("could not add connection {d} {any}", .{ connection.id, err });
            return;
        };

        std.log.debug("connections {d}", .{self.connections.count()});
        defer _ = self.removeConnection(connection.id);

        connection.run();

        std.log.debug("done running connection {d}", .{connection.id});
    }

    fn addConnection(self: *Self, conn: *Connection) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();
        try self.connections.put(conn.id, conn);
    }

    fn removeConnection(self: *Self, id: u128) bool {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();
        return self.connections.remove(id);
    }
};
