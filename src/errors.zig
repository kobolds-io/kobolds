pub const ProtocolError = error{
    /// the decoder was unable to decode the message because missing data
    /// this is either because the data provided was less than required to cast to
    /// a Header or there is not enough data for the body.
    NotEnoughData,
    /// The Header.header_checksum checksum does not match the expected checksum
    InvalidHeadersChecksum,
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
    // The version of the protocol is not supported and cannot be handled
    UnsupportedProtocol,
    // Length of the topic is invalid
    InvalidTopicLength,
    // Message does not support this operation
    InvalidMessageOperation,
    // A duplicate transaction id may not exist (at least at a service leve)
    DuplicateTransactionId,
};
