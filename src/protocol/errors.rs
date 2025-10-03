/// Errors while parsing RESP responses
#[derive(Debug, thiserror::Error)]
pub enum RedisParseError {
    // #[error("")]
    // UnexpectedEnd,
    #[error("Unknown starting byte '{}'", *_0 as char)]
    UnknownStartingByte(u8),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Invalid UTF-8 while parsing string")]
    InvalidUtf8,
    #[error("Failed to parse valid int")]
    ParseInt,
    #[error("Invalid bulk string size {0}")]
    BadBulkStringSize(i64),
    #[error("Invalid array size {0}")]
    BadArraySize(i64),
}
