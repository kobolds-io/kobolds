const std = @import("std");
const testing = std.testing;

pub fn EventEmitter(comptime Event: type, comptime Data: type) type {
    return struct {
        const Self = @This();

        pub const ListenerCallback = *const fn (event: Event, data: Data) void;

        pub const Listener = struct {
            callback: ListenerCallback,
        };

        allocator: std.mem.Allocator,
        listeners: std.AutoHashMap(Event, *std.ArrayList(Listener)),
        mutex: std.Thread.Mutex,

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .allocator = allocator,
                .listeners = std.AutoHashMap(Event, *std.ArrayList(Listener)).init(allocator),
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            var listeners_iter = self.listeners.valueIterator();
            while (listeners_iter.next()) |listener_list_ptr| {
                const listener_list = listener_list_ptr.*;

                // deinit and destroy the list
                listener_list.deinit();
                self.allocator.destroy(listener_list);
            }

            self.listeners.deinit();
        }

        pub fn addEventListener(self: *Self, event: Event, callback: ListenerCallback) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.listeners.get(event)) |listeners_list| {
                try listeners_list.append(.{ .callback = callback });
            } else {
                // create a new list
                const listener_list = try self.allocator.create(std.ArrayList(Listener));
                errdefer self.allocator.destroy(listener_list);

                listener_list.* = try std.ArrayList(Listener).initCapacity(self.allocator, 1);
                errdefer listener_list.deinit();

                listener_list.appendAssumeCapacity(.{ .callback = callback });

                try self.listeners.put(event, listener_list);
            }
        }

        pub fn removeEventListener(self: *Self, event: Event, callback: ListenerCallback) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.listeners.get(event)) |listener_list| {
                for (listener_list.items, 0..listener_list.items.len) |listener, i| {
                    if (listener.callback == callback) {
                        _ = listener_list.swapRemove(i);
                        return true;
                    }
                }
            }

            return false;
        }

        pub fn emit(self: *Self, event: Event, data: Data) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.listeners.get(event)) |listener_list| {
                for (listener_list.items) |listener| {
                    listener.callback(event, data);
                }
            }
        }
    };
}

const TestEvent = enum {
    open,
    close,
};

var test_number: u32 = 0;

test "emits events to all listeners" {
    const allocator = testing.allocator;
    var ee = EventEmitter(TestEvent, u32).init(allocator);
    defer ee.deinit();

    const callback1 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number += data;
        }
    }.callback;

    const callback2 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number += data;
        }
    }.callback;

    const callback3 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number += data;
        }
    }.callback;

    try ee.addEventListener(.open, callback1);
    try ee.addEventListener(.open, callback2);
    try ee.addEventListener(.open, callback3);

    ee.emit(.open, 1);

    try testing.expectEqual(3, test_number);

    try testing.expectEqual(true, ee.removeEventListener(.open, callback3));

    ee.emit(.open, 10);

    try testing.expectEqual(23, test_number);
}

const TestThreadEventEmitter = struct {
    const Self = @This();
    allocator: std.mem.Allocator,
    ee: EventEmitter(TestEvent, u32),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .ee = EventEmitter(TestEvent, u32).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.ee.deinit();
    }

    pub fn run(self: *Self) void {
        var published_events: u32 = 0;
        while (true) {
            if (published_events == 100) {
                return;
            }

            if (self.ee.listeners.count() > 0) {
                published_events += 1;
                self.ee.emit(.open, 1);
            }
        }
    }
};

var test_number_2: u32 = 0;

test "emits events over threads" {
    const allocator = testing.allocator;

    const callback1 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number_2 += data;
        }
    }.callback;

    const callback2 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number_2 += data;
        }
    }.callback;

    const callback3 = struct {
        pub fn callback(event: TestEvent, data: u32) void {
            _ = event;
            test_number_2 += data;
        }
    }.callback;

    var t = TestThreadEventEmitter.init(allocator);
    defer t.deinit();

    try t.ee.addEventListener(.open, callback1);
    try t.ee.addEventListener(.open, callback2);
    try t.ee.addEventListener(.open, callback3);

    const th = try std.Thread.spawn(.{}, TestThreadEventEmitter.run, .{&t});

    th.join();

    try testing.expectEqual(300, test_number_2);
}
