const Advertiser = @import("./advertiser.zig").Advertiser;
const Requestor = @import("./requestor.zig").Requestor;

pub const Transaction = struct {
    requestor: *Requestor,
    advertiser: *Advertiser,
    transaction_id: u128,
    recieved_at: i64,
    timeout: i64,
};
