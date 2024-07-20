const std = @import("std");
const uuid = @import("uuid");

const Connection = @import("./connection.zig").Connection;
const Message = @import("./message.zig").Message;
const Mailbox = @import("./mailbox.zig").Mailbox;

pub const ClusterOpts = struct {
    node_host: []const u8 = "127.0.0.1",
    node_port: u16 = 4000,
};

pub const Cluster = struct {
    const Self = @This();
    id: u128,
    node_host: []const u8,
    node_port: u16,
    connections: std.AutoHashMap(u128, *Connection) = undefined,
    connections_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, opts: ClusterOpts) !Cluster {
        // we should fail if the host port is not  0 >= port <= 65535
        if (opts.node_host.len == 0) return error.InvalidHost;
        if (opts.node_port <= 0 or opts.node_port >= 65535) return error.InvalidPort;

        return Cluster{
            .id = uuid.v7.new(),
            .node_host = opts.node_host,
            .node_port = opts.node_port,
            .connections = std.AutoHashMap(u128, *Connection).init(allocator),
            .connections_mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        // this will remove the references to each of these
        // but we need to still spin down everything that each
        // conneciton is doing

        var valueIter = self.connections.valueIterator();
        var conn: ?**Connection = valueIter.next();
        while (conn != null) {
            conn.?.*.deinit();
            conn = valueIter.next();
        }

        self.connections.deinit();
    }

    pub fn listen(self: *Self) !void {
        const address = try std.net.Address.parseIp(self.node_host, self.node_port);

        var listener = try address.listen(.{ .reuse_port = true });
        defer listener.deinit();

        std.debug.print("listening for new node connections on {any}\n", .{address});
        var connection_pool_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const connection_pool_allocator = connection_pool_gpa.allocator();
        defer _ = connection_pool_gpa.deinit();

        var connection_pool: std.Thread.Pool = undefined;
        try connection_pool.init(.{ .allocator = connection_pool_allocator });
        defer connection_pool.deinit();

        var connection_gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const connection_allocator = connection_gpa.allocator();
        defer _ = connection_gpa.deinit();

        while (listener.accept()) |server_connection| {
            const connection = try Connection.create(connection_allocator, server_connection.stream);

            // TODO: need a mechanic to remove the connection
            try self.addConnection(connection);

            try connection_pool.spawn(Connection.run, .{connection});
        } else |err| {
            std.log.err("failed to accept connection {}", .{err});
        }
    }

    fn addConnection(self: *Self, conn: *Connection) !void {
        self.connections_mutex.lock();
        defer self.connections_mutex.unlock();
        try self.connections.put(conn.id, conn);
    }
};

test "the lifecycle" {
    // var cluster = try Cluster.init(std.testing.allocator, .{});
    // defer cluster.deinit();
    //
    // var conn = Connection.init(std.testing.allocator, "my test connection");
    // defer conn.deinit();
    //
    // try cluster.connections.put("my_conn", &conn);
    // const retrieved_conn = cluster.connections.get("my_conn");
    //
    // try std.testing.expect(retrieved_conn != null);
    // try std.testing.expect(std.mem.eql(u8, retrieved_conn.?.id, conn.id));
    // try std.testing.expectEqual(1, cluster.connections.count());
}

test "it listens for new connections" {
    // var cluster = try Cluster.init(std.testing.allocator, .{});
    // defer cluster.deinit();
    //
    // try cluster.listen();

    // const cluster_th = try std.Thread.spawn(.{}, Cluster.listen, .{&cluster});

    // wait for the cluster to be joined

    // for (0..cpus) |i| {
    //     handles[i] = try std.Thread.spawn(.{}, work, .{i});
    // }
    //
    // for (handles) |h| h.join();
    // TODO: spawn a thread so the cluster listens for connections
    // TODO: spawn a client thread connect and disconnect
    // TODO: kill both threads
    // cluster_th.join();
}
