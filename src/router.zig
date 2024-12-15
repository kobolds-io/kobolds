const std = @import("std");

/// Router is a structure who's underlying message structure is used to ensure that messages
/// are sent to their correct destinations. The router will put messages into the outboxes
/// of connections
const Router = struct {
    // connections:

    // map: {
    //      origin_id: {
    //          conn: *Connection,
    //          outbox: *std.ArrayList(*Message)
    //      }
    //   }
    //
    // connection: *Connection,
    //      outbox: std.ArrayList(*Message),

    pub fn tick(self: *Router) !void {
        _ = self;
    }
};
