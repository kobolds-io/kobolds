pub const ProtocolError = error{
    /// the decoder was unable to decode the message because missing data
    /// this is either because the data provided was less than required to cast to
    /// a Header or there is not enough data for the body.
    NotEnoughData,
    /// The Header.header_checksum checksum does not match the expected checksum
    InvalidHeaderChecksum,
    /// The Header.body_checksum checksum does not match the expected checksum
    InvalidBodyChecksum,
    /// Already connected
    AlreadyConnected,
    /// Connection was closed
    ConnectionClosed,
    /// There was an issue reading from the connection
    ConnectionReadError,
    /// Connection is running
    ConnectionRunning,
};
