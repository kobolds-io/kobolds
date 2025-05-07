const std = @import("std");

const Bus = @import("./bus.zig").Bus;

pub fn BusManager(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        buses: std.StringHashMap(*Bus(T)),
        mutex: std.Thread.Mutex,

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .allocator = allocator,
                .buses = std.StringHashMap(*Bus(T)).init(allocator),
                .mutex = std.Thread.Mutex{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            var buses_iterator = self.buses.valueIterator();
            while (buses_iterator.next()) |entry| {
                const bus = entry.*;

                bus.deinit();
                self.allocator.destroy(bus);
            }

            self.buses.deinit();
        }

        pub fn findOrCreate(self: *Self, topic_name: []const u8) !*Bus(T) {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.buses.get(topic_name)) |bus| {
                return bus;
            } else {
                const bus = try self.allocator.create(Bus(T));
                errdefer self.allocator.destroy(bus);

                bus.* = try Bus(T).init(self.allocator, topic_name, 5_000);
                errdefer bus.deinit();

                try self.buses.put(topic_name, bus);

                return bus;
            }
        }

        pub fn destroy(self: *Self, topic_name: []const u8) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.buses.get(topic_name)) |bus| {
                bus.deinit();
                self.allocator.destroy(bus);

                return self.buses.remove(topic_name);
            }

            return false;
        }
    };
}
