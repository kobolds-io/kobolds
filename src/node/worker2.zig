const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const log = std.log.scoped(.Worker);

const UnbufferedChannel = @import("stdx").UnbufferedChannel;

const Node = @import("./node2.zig").Node;
const Message = @import("../protocol/message.zig").Message;

const WorkerState = enum {
    running,
    closing,
    closed,
};

pub const Worker = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    close_channel: *UnbufferedChannel(bool),
    done_channel: *UnbufferedChannel(bool),
    state: WorkerState,
    id: u32,
    node: *Node,

    pub fn init(allocator: std.mem.Allocator, id: u32, node: *Node) !Self {
        const close_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(close_channel);

        close_channel.* = UnbufferedChannel(bool).new();

        const done_channel = try allocator.create(UnbufferedChannel(bool));
        errdefer allocator.destroy(done_channel);

        done_channel.* = UnbufferedChannel(bool).new();

        return Self{
            .id = id,
            .allocator = allocator,
            .close_channel = close_channel,
            .done_channel = done_channel,
            .state = .closed,
            .node = node,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.close_channel);
        self.allocator.destroy(self.done_channel);
    }

    pub fn run(self: *Self, ready_channel: *UnbufferedChannel(bool)) void {
        // Notify the calling thread that the run loop is ready
        ready_channel.send(true);
        self.state = .running;
        log.info("worker {}: running", .{self.id});
        while (true) {
            // check if the close channel has received a close command
            const close_channel_received = self.close_channel.timedReceive(0) catch false;
            if (close_channel_received) {
                log.info("worker {} closing", .{self.id});
                self.state = .closing;
            }
            switch (self.state) {
                .running => {
                    self.tick() catch unreachable;
                    // TODO: add io_uring
                },
                .closing => {
                    log.info("worker {}: closed", .{self.id});
                    self.state = .closed;
                    self.done_channel.send(true);
                    return;
                },
                else => {
                    @panic("unable to tick closed worker");
                },
            }
        }
    }

    pub fn close(self: *Self) void {
        switch (self.state) {
            .closed, .closing => return,
            else => {
                // block until this is received by the background thread
                self.close_channel.send(true);
            },
        }

        // block until the worker fully exits
        _ = self.done_channel.receive();
    }

    pub fn tick(self: *Self) !void {
        _ = self;
    }
};
