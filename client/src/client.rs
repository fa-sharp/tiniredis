use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt, stream};
use tinikeyval_protocol::{RespCodec, RespValue};
use tokio::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};
use tokio_util::codec::Framed;

use crate::{ClientConfig, error::ClientError as Error, value::Value};

/// Redis client that holds a reference to a connection. Cheaply cloneable.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<Framed<BufWriter<BufReader<TcpStream>>, RespCodec>>>,
    config: ClientConfig,
}

impl Client {
    /// Connect to the Redis server and create a client with default configuration
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let config = ClientConfig::default();
        Self::connect_with_config(url, config).await
    }

    /// Connect to the Redis server and create a client with the given configuration
    pub async fn connect_with_config(url: &str, config: ClientConfig) -> Result<Self, Error> {
        let tcp_stream = TcpStream::connect(url).await?;
        let mut cxn = RespCodec::framed_io(BufWriter::new(BufReader::new(tcp_stream)));

        const PING: Bytes = Bytes::from_static(b"PING");
        cxn.send(RespValue::Array(vec![RespValue::String(PING)]))
            .await?;
        match timeout(config.timeout, cxn.try_next()).await?? {
            Some(pong) => pong,
            None => Err(Error::Disconnected)?,
        };

        let inner = Arc::new(Mutex::new(cxn));
        Ok(Self { inner, config })
    }

    /// Send a raw command to the Redis server and get the response
    pub async fn send<S>(&self, command: Vec<S>) -> Result<Value, Error>
    where
        S: AsRef<str>,
    {
        let raw_command = RespValue::Array(command.into_iter().map(str_to_bulk_string).collect());
        let mut cxn = self.inner.lock().await;
        cxn.send(raw_command).await?;
        let raw_response = timeout(self.config.timeout, cxn.try_next())
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
            Ok(RespValue::Array(
                command.into_iter().map(str_to_bulk_string).collect(),
            ))
        });
        let mut cxn = self.inner.lock().await;
        cxn.send_all(&mut stream::iter(raw_commands)).await?;
        let responses = timeout(
            self.config.timeout,
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

    /// Subscribe to the given pubsub channels. Creates a new connection to prevent
    /// blocking the current connection, and returns a stream of `(channel, message)`.
    pub async fn subscribe<S>(
        &self,
        channels: Vec<S>,
    ) -> Result<impl Stream<Item = Result<(Bytes, Bytes), Error>> + use<S>, Error>
    where
        S: AsRef<str>,
    {
        let addr = {
            let inner = self.inner.lock().await;
            inner.get_ref().get_ref().get_ref().peer_addr()?
        };
        let sub_client = Self::connect_with_config(&addr.to_string(), self.config.clone()).await?;

        let command = std::iter::once("SUBSCRIBE").chain(channels.iter().map(S::as_ref));
        let raw_command = RespValue::Array(command.into_iter().map(str_to_bulk_string).collect());
        sub_client.inner.lock().await.send(raw_command).await?;

        let inner_stream = Arc::try_unwrap(sub_client.inner)
            .expect("should only have one reference")
            .into_inner()
            .into_stream();
        let messages_stream = inner_stream
            .skip(channels.len()) // skip confirmation messages
            .map(|parse_result| match parse_result {
                Ok(raw_val) => match raw_val {
                    RespValue::Array(mut values) => {
                        let message = values
                            .pop()
                            .and_then(RespValue::into_bytes)
                            .ok_or(Error::Invalid("No message".into()))?;
                        let channel = values
                            .pop()
                            .and_then(RespValue::into_bytes)
                            .ok_or(Error::Invalid("No channel".into()))?;
                        Ok((channel, message))
                    }
                    _ => Err(Error::Invalid(format!("Expected array, got {:?}", raw_val))),
                },
                Err(err) => Err(Error::Parse(err)),
            });

        Ok(messages_stream)
    }
}

fn str_to_bulk_string<S: AsRef<str>>(s: S) -> RespValue {
    RespValue::String(Bytes::copy_from_slice(s.as_ref().as_bytes()))
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

    #[tokio::test]
    async fn subscribe() -> ClientResult<()> {
        let client = Client::connect(LOCALHOST).await?;
        let mut message_stream = client.subscribe(vec!["foo", "bar"]).await?;

        tokio::spawn(async move {
            let _ = client.send(vec!["PUBLISH", "foo", "Hello"]).await;
            let _ = client.send(vec!["PUBLISH", "bar", "Goodbye"]).await;
        });

        assert_eq!(
            message_stream.try_next().await?,
            Some((Bytes::from("foo"), Bytes::from("Hello")))
        );
        assert_eq!(
            message_stream.try_next().await?,
            Some((Bytes::from("bar"), Bytes::from("Goodbye")))
        );

        Ok(())
    }
}
