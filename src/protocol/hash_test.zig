const std = @import("std");
const testing = std.testing;

const hash = @import("./hash.zig");
const checksumV1 = hash.checksumV1;
const checksumV2 = hash.checksumV2;
const checksumV3 = hash.checksumV3;
const verifyV2 = hash.verifyV2;
const verifyV3 = hash.verifyV3;

test checksumV1 {
    // hello
    const test_1 = [16]u8{ 189, 162, 182, 173, 7, 125, 53, 108, 149, 223, 251, 10, 0, 201, 124, 17 };
    // this is quite a long string with a long and storied past that is sure to make yee tremble in yee boots!
    const test_2 = [16]u8{ 89, 236, 255, 3, 84, 148, 77, 106, 249, 73, 4, 4, 65, 1, 36, 77 };

    try testing.expect(std.mem.eql(
        u8,
        &test_1,
        &checksumV1("hello"),
    ));

    try testing.expect(std.mem.eql(
        u8,
        &test_2,
        &checksumV1("this is quite a long string with a long and storied past that is sure to make yee tremble in yee boots!"),
    ));
}

test checksumV2 {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    const test_1_want: u128 = 321637061829959228085805028512378412621;
    const test_2_want: u128 = 225562635746912573106789807876563323021;

    try testing.expectEqual(test_1_want, checksumV2(&test_1));
    try testing.expectEqual(test_2_want, checksumV2(&test_2));
}

test verifyV2 {
    const test_1 = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [16]u8{ 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    const test_1_want: u128 = 321637061829959228085805028512378412621;
    const test_2_want: u128 = 225562635746912573106789807876563323021;

    try testing.expect(verifyV2(test_1_want, &test_1));
    try testing.expect(verifyV2(test_2_want, &test_2));
}

test checksumV3 {
    const test_1 = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [8]u8{ 8, 7, 6, 5, 4, 3, 2, 1 };
    const test_1_want: u64 = 8816792505169454855;
    const test_2_want: u64 = 8835605119511905190;

    try testing.expectEqual(test_1_want, checksumV3(&test_1));
    try testing.expectEqual(test_2_want, checksumV3(&test_2));
}

test verifyV3 {
    const test_1 = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    const test_2 = [8]u8{ 8, 7, 6, 5, 4, 3, 2, 1 };
    const test_1_want: u64 = 8816792505169454855;
    const test_2_want: u64 = 8835605119511905190;

    try testing.expect(verifyV3(test_1_want, &test_1));
    try testing.expect(verifyV3(test_2_want, &test_2));
}
