const std = @import("std");
const zbench = @import("zbench");

const Parser = @import("./protocol/parser.zig").Parser;
const Message = @import("./protocol/message.zig").Message;
const constants = @import("./protocol/constants.zig");

const ParserBenchmark = struct {
    messages: *std.ArrayList(Message),
    parser: *Parser,

    fn new(messages: *std.ArrayList(Message), parser: *Parser) ParserBenchmark {
        return .{
            .messages = messages,
            .parser = parser,
        };
    }

    pub fn run(self: ParserBenchmark, _: std.mem.Allocator) void {
        const body = [_]u8{97} ** constants.message_max_body_size;
        const encoded_message = ([_]u8{ 210, 178, 72, 61, 116, 58, 115, 70, 241, 21, 141, 51, 234, 162, 146, 179, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } ++ body);

        self.parser.parse(self.messages, &encoded_message) catch unreachable;
    }
};

pub fn BenchmarkMessageEncode(_: std.mem.Allocator) void {
    var backing_buf: [constants.message_max_size]u8 = undefined;

    const body = comptime "a" ** constants.message_max_body_size;
    var message = Message.new();
    message.setBody(body);

    message.encode(backing_buf[0..message.size()]);
}

pub fn BenchmarkMessageDecode(_: std.mem.Allocator) void {
    const body = [_]u8{97} ** constants.message_max_body_size;
    const encoded_message = [_]u8{ 180, 53, 75, 231, 147, 154, 254, 149, 112, 23, 160, 125, 67, 13, 103, 92, 0, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 } ++ body;

    var message = Message.new();
    message.decode(&encoded_message) catch unreachable;
}

test {
    // var bench = zbench.Benchmark.init(std.testing.allocator, .{});
    var bench = zbench.Benchmark.init(std.testing.allocator, .{ .iterations = std.math.maxInt(u16) });
    defer bench.deinit();

    var messages_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = messages_gpa.deinit();
    const messages_allocator = messages_gpa.allocator();

    var messages = std.ArrayList(Message).initCapacity(messages_allocator, std.math.maxInt(u16)) catch unreachable;
    defer messages.deinit();

    var parser_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = parser_gpa.deinit();
    const parser_allocator = parser_gpa.allocator();

    // var parser_buf: [constants.parser_max_buffer_size]u8 = undefined;
    // var parser_fba = std.heap.FixedBufferAllocator.init(&parser_buf);
    // const parser_allocator = parser_fba.allocator();

    var parser = Parser.init(parser_allocator);
    defer parser.deinit();

    try bench.add("message.encode", BenchmarkMessageEncode, .{});
    try bench.add("message.decode", BenchmarkMessageDecode, .{});
    try bench.addParam("parser.parse", &ParserBenchmark.new(&messages, &parser), .{});

    const stderr = std.io.getStdErr().writer();

    try stderr.writeAll("\n");

    try bench.run(stderr);
    try stderr.print("{}\n", .{try zbench.getSystemInfo()});

    // ---------------------------------------------------

    // uncomment this to write to a file then use jq to cut it up into useable chunks
    // jq -c '.[]' benchmark.json | while read -r item; do name=$(echo "$item" | jq -r '.name'); echo "$item" | jq -r '.timings[]' > "${name// /_}.csv"; done

    // const file = try std.fs.cwd().createFile("benchmark.json", .{});
    // const writer = file.writer();
    // var json_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = json_gpa.deinit();
    //
    // try writer.writeAll("[");
    // var iter = try bench.iterator();
    // var i: usize = 0;
    // while (try iter.next()) |step| switch (step) {
    //     .progress => |_| {},
    //     .result => |x| {
    //         defer x.deinit();
    //         defer i += 1;
    //         if (0 < i) try writer.writeAll(", ");
    //         try x.writeJSON(json_gpa.allocator(), writer);
    //     },
    // };
    // try writer.writeAll("]\n");
}
