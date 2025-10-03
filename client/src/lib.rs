use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, TryStreamExt};
use tinikeyval::protocol::{RedisParseError, RedisValue, RespCodec};
use tokio::{
    io::{self, BufReader, BufWriter},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};
use tokio_util::codec::Framed;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Connection: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse: {0}")]
    Parse(#[from] RedisParseError),
    #[error("Disconnected")]
    Disconnected,
}

pub type ClientResult<T> = Result<T, Error>;

pub struct Client {
    inner: Mutex<ClientInner>,
}

struct ClientInner {
    cxn: Framed<BufWriter<BufReader<TcpStream>>, RespCodec>,
}

const TIMEOUT: Duration = Duration::from_secs(5);
const PING: Bytes = Bytes::from_static(b"PING");

impl Client {
    /// Connect to the Redis server and create a client
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let tcp_stream = TcpStream::connect(url).await?;
        let mut cxn = RespCodec::framed_io(io::BufWriter::new(io::BufReader::new(tcp_stream)));

        // Ping the server to verify connection
        cxn.send(RedisValue::Array(vec![RedisValue::String(PING)]))
            .await?;
        match timeout(TIMEOUT, cxn.try_next()).await?? {
            Some(pong) => pong,
            None => Err(Error::Disconnected)?,
        };

        Ok(Self {
            inner: Mutex::new(ClientInner { cxn }),
        })
    }

    /// Send a raw command to the Redis server and get the response
    pub async fn send(&self, command: Vec<Bytes>) -> Result<RedisValue, Error> {
        let mut inner = self.inner.lock().await;
        let command_val = RedisValue::Array(command.into_iter().map(RedisValue::String).collect());
        inner.cxn.send(command_val).await?;
        timeout(TIMEOUT, inner.cxn.try_next())
            .await??
            .ok_or(Error::Disconnected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect() -> ClientResult<()> {
        let _ = Client::connect("127.0.0.1:6379").await?;
        Ok(())
    }

    #[tokio::test]
    async fn ping() -> ClientResult<()> {
        let client = Client::connect("127.0.0.1:6379").await?;
        let pong = client.send(vec![PING]).await?;
        assert_eq!(pong, RedisValue::String(Bytes::from_static(b"PONG")));

        Ok(())
    }
}
