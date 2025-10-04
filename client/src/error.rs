use tinikeyval_protocol::RedisParseError;

/// Result type for the Redis client
pub type ClientResult<T> = Result<T, Error>;

/// Possible errors when communicating with the Redis server
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Connection: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse response: {0}")]
    Parse(#[from] RedisParseError),
    #[error("Error response: {0:?}")]
    ResponseError(bytes::Bytes),
    #[error("Disconnected")]
    Disconnected,
}
