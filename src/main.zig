const std = @import("std");

pub fn main() !void {
    // const data: [4]u8 = [4]u8{0x12, 0x34, 0x56, 0x78};
    const data: []const u8 = &[_]u8{ 0, 0, 0, 5, 104, 101, 108, 108, 111 };
    const result = bytesToU32BigEndian(data[0..4]) catch {
        std.debug.print("Error: Invalid byte array length\n", .{});
        return;
    };

    std.debug.print("The u32 value is: {x}\n", .{result});
}

// Todo convert 4 bytes to u32 from bigendian
fn bytesToU32BigEndian(bytes: []const u8) !u32 {
    if (bytes.len != 4) {
        return error.InvalidByteArrayLength;
    }

    const a: u8 = @intCast(bytes[0] << 24);
    const b: u8 = @intCast(bytes[1] << 16);
    const c: u8 = @intCast(bytes[2] << 8);
    const d: u8 = @intCast(bytes[3]);

    return a | b | c | d;
}
