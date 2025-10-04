use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryStreamExt, stream};
use tinikeyval_protocol::{RedisValue, RespCodec};
use tokio::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{error::Error, value::Value};

/// Redis client that holds a reference to a connection. Cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<Framed<BufWriter<BufReader<TcpStream>>, RespCodec>>>,
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
            inner: Mutex::new(cxn).into(),
        })
    }

    /// Send a raw command to the Redis server and get the response
    pub async fn send<S>(&self, command: Vec<S>) -> Result<Value, Error>
    where
        S: AsRef<str>,
    {
        let raw_command = RedisValue::Array(command.into_iter().map(str_to_bulk_string).collect());
        let mut cxn = self.inner.lock().await;
        cxn.send(raw_command).await?;
        let raw_response = timeout(TIMEOUT, cxn.try_next())
            .await??
            .ok_or(Error::Disconnected)?;

        Value::try_from(raw_response)
    }

    /// Send a raw pipeline to the Redis server and get an array of results
    pub async fn pipeline<S>(
        &self,
        commands: Vec<Vec<S>>,
    ) -> Result<Vec<Result<Value, Error>>, Error>
    where
        S: AsRef<str>,
    {
        let num_commands = commands.len();
        let raw_commands = commands.into_iter().map(|command| {
            Ok(RedisValue::Array(
                command.into_iter().map(str_to_bulk_string).collect(),
            ))
        });
        let mut cxn = self.inner.lock().await;
        cxn.send_all(&mut stream::iter(raw_commands)).await?;
        let responses = timeout(
            TIMEOUT,
            cxn.by_ref()
                .take(num_commands)
                .map(|parse_result| match parse_result {
                    Ok(raw) => Value::try_from(raw),
                    Err(err) => Err(Error::Parse(err)),
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        Ok(responses)
    }
}

fn str_to_bulk_string<S: AsRef<str>>(s: S) -> RedisValue {
    RedisValue::String(Bytes::copy_from_slice(s.as_ref().as_bytes()))
}

#[cfg(test)]
mod tests {
    use futures::future::try_join_all;
    use tinikeyval_protocol::constants;

    use crate::error::ClientResult;

    use super::*;

    const LOCALHOST: &str = "127.0.0.1:6379";
    const PONG: Value = Value::String(Bytes::from_static(b"PONG"));

    #[tokio::test]
    async fn connect() -> ClientResult<()> {
        let _ = Client::connect(LOCALHOST).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ping() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let pong = client.send(vec!["PING"]).await?;
        assert_eq!(pong, PONG);

        Ok(())
    }

    #[tokio::test]
    async fn get_and_set() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let res = client.send(vec!["SET", "foo", "bar"]).await?;
        assert_eq!(res, constants::OK.try_into()?);

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
        assert!(matches!(res, Value::Int(_)));

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

    #[tokio::test]
    async fn pipeline() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let commands = vec![
            vec!["PING"],
            vec!["SET", "foo", "bar"],
            vec!["INVALID"],
            vec!["GET", "foo"],
        ];
        let responses = client.pipeline(commands).await?;
        assert_eq!(responses.len(), 4);
        assert_eq!(responses[0].as_ref().unwrap(), &PONG);
        assert_eq!(responses[1].as_ref().unwrap(), &constants::OK.try_into()?);
        assert!(matches!(responses[2], Err(Error::ResponseError(_))));
        assert_eq!(
            responses[3].as_ref().unwrap(),
            &Value::String(Bytes::from_static(b"bar"))
        );

        // Verify that the client connection is still open and functional
        assert_eq!(client.send(vec!["PING"]).await?, PONG);

        Ok(())
    }

    #[tokio::test]
    async fn join_responses() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let results = try_join_all(vec![
            client.send(vec!["PING"]),
            client.send(vec!["PING"]),
            client.send(vec!["PING"]),
        ])
        .await?;
        assert_eq!(results, vec![PONG; 3]);

        Ok(())
    }
}
