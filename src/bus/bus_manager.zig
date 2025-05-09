const std = @import("std");

const constants = @import("../constants.zig");

const Bus = @import("./bus.zig").Bus;

pub const BusManager = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    buses: std.StringHashMap(*Bus),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .buses = std.StringHashMap(*Bus).init(allocator),
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

    pub fn findOrCreate(self: *Self, topic_name: []const u8) !*Bus {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.buses.get(topic_name)) |bus| {
            return bus;
        } else {
            const bus = try self.allocator.create(Bus);
            errdefer self.allocator.destroy(bus);

            bus.* = try Bus.init(
                self.allocator,
                topic_name,
                constants.bus_max_queue_capacity,
            );
            errdefer bus.deinit();

            try self.buses.put(topic_name, bus);

            return bus;
        }
    }

    pub fn get(self: *Self, topic_name: []const u8) ?*Bus {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.buses.get(topic_name);
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
