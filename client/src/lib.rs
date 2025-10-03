use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, TryStreamExt};
use tinikeyval::protocol::{RedisParseError, RedisValue, RespCodec};
use tokio::{
    io::{BufReader, BufWriter},
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
    #[error("Error response from Redis: {0:?}")]
    ResponseError(Bytes),
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

/// A parsed Redis value
#[derive(Debug, PartialEq)]
pub enum Value {
    String(Bytes),
    Array(Vec<Value>),
    Int(i64),
    Nil,
}

impl TryFrom<RedisValue> for Value {
    type Error = Error;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        Ok(match value {
            RedisValue::String(bytes) | RedisValue::SimpleString(bytes) => Value::String(bytes),
            RedisValue::Error(bytes) => Err(Error::ResponseError(bytes))?,
            RedisValue::Int(i) => Value::Int(i),
            RedisValue::Array(values) => {
                let converted = values.into_iter().map(Value::try_from);
                Value::Array(converted.collect::<Result<_, _>>()?)
            }
            RedisValue::NilArray | RedisValue::NilString => Value::Nil,
        })
    }
}

const TIMEOUT: Duration = Duration::from_secs(6);
const PING: Bytes = Bytes::from_static(b"PING");

impl Client {
    /// Connect to the Redis server and create a client
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let tcp_stream = TcpStream::connect(url).await?;
        let mut cxn = RespCodec::framed_io(BufWriter::new(BufReader::new(tcp_stream)));

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
    pub async fn send<S>(&self, command: Vec<S>) -> Result<Value, Error>
    where
        S: AsRef<str>,
    {
        fn str_to_bulk_string<S: AsRef<str>>(s: S) -> RedisValue {
            RedisValue::String(Bytes::copy_from_slice(s.as_ref().as_bytes()))
        }
        let raw_command = RedisValue::Array(command.into_iter().map(str_to_bulk_string).collect());
        let mut inner = self.inner.lock().await;
        inner.cxn.send(raw_command).await?;
        let raw_response = timeout(TIMEOUT, inner.cxn.try_next())
            .await??
            .ok_or(Error::Disconnected)?;

        Value::try_from(raw_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCALHOST: &str = "127.0.0.1:6379";

    #[tokio::test]
    async fn connect() -> ClientResult<()> {
        let _ = Client::connect(LOCALHOST).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ping() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let pong = client.send(vec!["PING"]).await?;
        assert_eq!(pong, Value::String(Bytes::from_static(b"PONG")));

        Ok(())
    }

    #[tokio::test]
    async fn get_and_set() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let res = client.send(vec!["SET", "foo", "bar"]).await?;
        assert_eq!(res, Value::String(Bytes::from_static(b"OK")));

        let res = client.send(vec!["GET", "foo"]).await?;
        assert_eq!(res, Value::String(Bytes::from_static(b"bar")));

        Ok(())
    }

    #[tokio::test]
    async fn sorted_set() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let res = client
            .send(vec!["ZADD", "scores", "1", "foo", "5", "bar", "3", "baz"])
            .await?;
        assert_eq!(res, Value::Int(3));

        let res = client.send(vec!["ZRANGE", "scores", "0", "-1"]).await?;
        assert_eq!(
            res,
            Value::Array(vec![
                Value::String(Bytes::from_static(b"foo")),
                Value::String(Bytes::from_static(b"baz")),
                Value::String(Bytes::from_static(b"bar"))
            ])
        );

        Ok(())
    }
}
