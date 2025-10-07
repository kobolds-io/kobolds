const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");

const constants = @import("./constants.zig");
const hash = @import("./hash.zig");

pub fn u128ToBytes(value: u128) [16]u8 {
    var buf: [16]u8 = undefined;
    std.mem.writeInt(u128, &buf, value, .big);
    return buf;
}

pub fn u64ToBytes(value: u64) [8]u8 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    return buf;
}

pub fn u32ToBytes(value: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    return buf;
}

pub fn u16ToBytes(value: u16) [2]u8 {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    return buf;
}

pub fn bytesToU128(bytes: *const [16]u8) u128 {
    return std.mem.readInt(u128, bytes, .big);
}

pub fn bytesToU64(bytes: *const [8]u8) u64 {
    return std.mem.readInt(u64, bytes, .big);
}

pub fn bytesToU32(bytes: *const [4]u8) u32 {
    return std.mem.readInt(u32, bytes, .big);
}

pub fn bytesToU16(bytes: *const [2]u8) u16 {
    return std.mem.readInt(u16, bytes, .big);
}

pub fn generateKey64(topic_name: []const u8, id: u64) u64 {
    var buf: [constants.message_max_topic_name_size + @sizeOf(u64)]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const fba_allocator = fba.allocator();

    // a failure here would be unrecoverable
    var bytes_list = std.array_list.Managed(u8).initCapacity(fba_allocator, buf.len) catch unreachable;

    bytes_list.appendSliceAssumeCapacity(topic_name);
    bytes_list.appendSliceAssumeCapacity(&u64ToBytes(id));
    defer bytes_list.deinit();

    // we are just going to use the same checksum hasher as we do for messages.
    return hash.xxHash64Checksum(bytes_list.items);
}

pub fn generateKey128(topic_name: []const u8, id: u128) u128 {
    var buf: [constants.message_max_topic_name_size + @sizeOf(u128)]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const fba_allocator = fba.allocator();

    // a failure here would be unrecoverable
    var bytes_list = std.array_list.Managed(u8).initCapacity(fba_allocator, buf.len) catch unreachable;

    bytes_list.appendSliceAssumeCapacity(topic_name);
    bytes_list.appendSliceAssumeCapacity(&u128ToBytes(id));
    defer bytes_list.deinit();

    // we are just going to use the same checksum hasher as we do for messages.
    return hash.xxHash64Checksum(bytes_list.items);
}

pub fn generateUniqueId(salt: u128) u64 {
    const ns_timestamp = std.time.nanoTimestamp();

    var buf: [@sizeOf(i128) + @sizeOf(u128)]u8 = undefined;
    // create a fixed buffer allocator to write the values that should be checksummed
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const fba_allocator = fba.allocator();

    var list = std.array_list.Managed(u8).initCapacity(
        fba_allocator,
        @sizeOf(i128) + @sizeOf(u128),
    ) catch unreachable;

    // this excludes header_checksum & body_checksum
    list.appendSliceAssumeCapacity(&u128ToBytes(@intCast(ns_timestamp)));
    list.appendSliceAssumeCapacity(&u128ToBytes(salt));

    return hash.xxHash64Checksum(list.items);
}

// std.SemanticVersion requires there be no extra characters after the
// major/minor/patch numbers. But when we try to parse `uname
// --kernel-release` (note: while Linux doesn't follow semantic
// versioning, it doesn't violate it either), some distributions have
// extra characters, such as this Fedora one: 6.3.8-100.fc37.x86_64, and
// this WSL one has more than three dots:
// 5.15.90.1-microsoft-standard-WSL2.
pub fn parse_dirty_semver(dirty_release: []const u8) !std.SemanticVersion {
    const release = blk: {
        var last_valid_version_character_index: usize = 0;
        var dots_found: u8 = 0;
        for (dirty_release) |c| {
            if (c == '.') dots_found += 1;
            if (dots_found == 3) {
                break;
            }

            if (c == '.' or (c >= '0' and c <= '9')) {
                last_valid_version_character_index += 1;
                continue;
            }

            break;
        }

        break :blk dirty_release[0..last_valid_version_character_index];
    };

    return std.SemanticVersion.parse(release);
}

const fsblkcnt64_t = u64;
const fsfilcnt64_t = u64;
const fsword_t = i64;
const fsid_t = u64;
pub const TmpfsMagic = 0x01021994;

pub const StatFs = extern struct {
    f_type: fsword_t,
    f_bsize: fsword_t,
    f_blocks: fsblkcnt64_t,
    f_bfree: fsblkcnt64_t,
    f_bavail: fsblkcnt64_t,
    f_files: fsfilcnt64_t,
    f_ffree: fsfilcnt64_t,
    f_fsid: fsid_t,
    f_namelen: fsword_t,
    f_frsize: fsword_t,
    f_flags: fsword_t,
    f_spare: [4]fsword_t,
};

pub fn fstatfs(fd: i32, statfs_buf: *StatFs) usize {
    return std.os.linux.syscall2(
        if (@hasField(std.os.linux.SYS, "fstatfs64")) .fstatfs64 else .fstatfs,
        @as(usize, @bitCast(@as(isize, fd))),
        @intFromPtr(statfs_buf),
    );
}

/// Like std.fmt.bufPrint, but checks, at compile time, that the buffer is sufficiently large.
pub fn array_print(
    comptime n: usize,
    buffer: *[n]u8,
    comptime fmt: []const u8,
    args: anytype,
) []const u8 {
    const Args = @TypeOf(args);
    const ArgsStruct = @typeInfo(Args).Struct;
    comptime assert(ArgsStruct.is_tuple);

    comptime {
        var args_worst_case: Args = undefined;
        for (ArgsStruct.fields, 0..) |field, index| {
            const arg_worst_case = switch (field.type) {
                u64 => std.math.maxInt(field.type),
                else => @compileError("array_print: unhandled type"),
            };
            args_worst_case[index] = arg_worst_case;
        }
        const buffer_size = std.fmt.count(fmt, args_worst_case);
        assert(n >= buffer_size); // array_print buffer too small
    }

    return std.fmt.bufPrint(buffer, fmt, args) catch |err| switch (err) {
        error.NoSpaceLeft => unreachable,
    };
}

pub fn unexpected_errno(label: []const u8, err: std.posix.system.E) std.posix.UnexpectedError {
    std.log.scoped(.stdx).err("unexpected errno: {s}: code={d} name={?s}", .{
        label,
        @intFromEnum(err),
        std.enums.tagName(std.posix.system.E, err),
    });

    if (builtin.mode == .Debug) {
        std.debug.dumpCurrentStackTrace(null);
    }
    return error.Unexpected;
}

test u16ToBytes {
    const value1: u16 = 5;
    const bytes1 = u16ToBytes(value1);
    const want1 = [2]u8{ 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u16 = 256;
    const bytes2 = u16ToBytes(value2);
    const want2 = [2]u8{ 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u16 = 3000;
    const bytes3 = u16ToBytes(value3);
    const want3 = [2]u8{ 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}

test u32ToBytes {
    const value1: u32 = 5;
    const bytes1 = u32ToBytes(value1);
    const want1 = [4]u8{ 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u32 = 256;
    const bytes2 = u32ToBytes(value2);
    const want2 = [4]u8{ 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u32 = 3000;
    const bytes3 = u32ToBytes(value3);
    const want3 = [4]u8{ 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}

test u64ToBytes {
    const value1: u64 = 5;
    const bytes1 = u64ToBytes(value1);
    const want1 = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u64 = 256;
    const bytes2 = u64ToBytes(value2);
    const want2 = [8]u8{ 0, 0, 0, 0, 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u64 = 3000;
    const bytes3 = u64ToBytes(value3);
    const want3 = [8]u8{ 0, 0, 0, 0, 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}

test u128ToBytes {
    const value1: u128 = 5;
    const bytes1 = u128ToBytes(value1);
    const want1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5 };

    try std.testing.expect(std.mem.eql(u8, &want1, &bytes1));

    const value2: u128 = 256;
    const bytes2 = u128ToBytes(value2);
    const want2 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 };

    try std.testing.expect(std.mem.eql(u8, &want2, &bytes2));

    const value3: u128 = 3000;
    const bytes3 = u128ToBytes(value3);
    const want3 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 184 };

    try std.testing.expect(std.mem.eql(u8, &want3, &bytes3));
}
