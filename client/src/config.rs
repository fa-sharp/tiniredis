use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for the Redis client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Connection and response timeout. Default: 10 seconds
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
        }
    }
}
