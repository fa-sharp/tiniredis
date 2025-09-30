use std::collections::VecDeque;

use anyhow::bail;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use tokio::sync::oneshot;

use crate::{
    arguments::Arguments,
    parser::RedisValue,
    queues::Queues,
    senders::Senders,
    storage::{ListDirection, Storage},
    tasks::BPopClient,
};

/// Represents a parsed Redis command
#[derive(Debug)]
pub enum Command {
    Ping,
    DbSize,
    Echo {
        message: Bytes,
    },
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        val: Bytes,
        ttl: Option<u64>,
    },
    Push {
        key: Bytes,
        elems: VecDeque<Bytes>,
        dir: ListDirection,
    },
    Pop {
        key: Bytes,
        dir: ListDirection,
        count: i64,
    },
    BPop {
        key: Bytes,
        dir: ListDirection,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
}

/// The possible response from a command
pub enum CommandResponse {
    Value(RedisValue),
    Block(BoxFuture<'static, Result<RedisValue, oneshot::error::RecvError>>),
}
impl From<RedisValue> for CommandResponse {
    fn from(value: RedisValue) -> Self {
        Self::Value(value)
    }
}

impl Command {
    /// Parse the command from the raw input value
    pub fn from_value(raw_value: RedisValue) -> anyhow::Result<Self> {
        let mut args = Arguments::from_raw_value(raw_value)?;

        let command = match args.command() {
            "PING" => Self::Ping,
            "DBSIZE" => Self::DbSize,
            "ECHO" => {
                let message = args.pop("message")?;
                Self::Echo { message }
            }
            "GET" => {
                let key = args.pop("key")?;
                Self::Get { key }
            }
            "SET" => {
                let key = args.pop("key")?;
                let val = args.pop("value")?;
                let ex = args.pop_optional_named("EX");
                let px = args.pop_optional_named("PX");
                let ttl = match (ex, px) {
                    (None, None) => None,
                    (Some(ex), None) => Some(
                        std::str::from_utf8(&ex)?
                            .parse::<u64>()
                            .map(|secs| secs * 1000)?,
                    ),
                    (None, Some(px)) => Some(std::str::from_utf8(&px)?.parse()?),
                    (Some(_), Some(_)) => bail!("can't provide both EX and PX"),
                };
                Self::Set { key, val, ttl }
            }
            "RPUSH" | "LPUSH" => {
                let key = args.pop("key")?;
                let elem = args.pop("element")?;
                let mut elems: VecDeque<_> = vec![elem].into();
                while let Some(elem) = args.pop_optional() {
                    elems.push_back(elem);
                }
                let dir = match args.command() {
                    "RPUSH" => ListDirection::Right,
                    "LPUSH" => ListDirection::Left,
                    _ => unreachable!(),
                };
                Self::Push { key, elems, dir }
            }
            "RPOP" | "LPOP" => {
                let key = args.pop("key")?;
                let count = args.pop_optional_i64()?.unwrap_or(1);
                let dir = match args.command() {
                    "RPOP" => ListDirection::Right,
                    "LPOP" => ListDirection::Left,
                    _ => unreachable!(),
                };
                Self::Pop { key, dir, count }
            }
            "BRPOP" | "BLPOP" => {
                let key = args.pop("key")?;
                let dir = match args.command() {
                    "BRPOP" => ListDirection::Right,
                    "BLPOP" => ListDirection::Left,
                    _ => unreachable!(),
                };
                Self::BPop { key, dir }
            }
            "LRANGE" => {
                let key = args.pop("key")?;
                let start = args.pop_i64("start index")?;
                let stop = args.pop_i64("stop index")?;

                Self::LRange { key, start, stop }
            }
            cmd => bail!("Unrecognized command '{cmd}'"),
        };

        Ok(command)
    }

    /// Execute the command and get the response
    pub fn execute(
        self,
        storage: &mut impl Storage,
        queues: &Queues,
        senders: &Senders,
    ) -> CommandResponse {
        match self {
            Command::Ping => RedisValue::SimpleString(Bytes::from_static(b"PONG")).into(),
            Command::Echo { message } => RedisValue::String(message).into(),
            Command::DbSize => RedisValue::Int(storage.size()).into(),
            Command::Get { key } => match storage.get(&key) {
                Some(val) => RedisValue::String(val).into(),
                None => RedisValue::NilString.into(),
            },
            Command::Set { key, val, ttl } => {
                storage.set(key, val, ttl);
                RedisValue::SimpleString(Bytes::from_static(b"OK")).into()
            }
            Command::Push { key, elems, dir } => match storage.push(key.clone(), elems, dir) {
                Ok(len) => {
                    senders.notify_bpop(key); // notify blocking pop clients
                    RedisValue::Int(len).into()
                }
                Err(bytes) => RedisValue::Error(bytes).into(),
            },
            Command::Pop { key, dir, count } => match storage.pop(&key, dir, count) {
                Some(mut elems) => {
                    if count == 1 {
                        RedisValue::String(elems.pop().expect("should have 1 item")).into()
                    } else {
                        RedisValue::Array(elems.into_iter().map(RedisValue::String).collect())
                            .into()
                    }
                }
                None => RedisValue::NilString.into(),
            },
            Command::BPop { key, dir } => match storage.pop(&key, dir, 1) {
                Some(mut elems) => RedisValue::Array(vec![
                    RedisValue::String(key),
                    RedisValue::String(elems.pop().expect("should have 1 item")),
                ])
                .into(),
                None => {
                    let (tx, rx) = oneshot::channel();
                    let key_response = key.clone();
                    queues.bpop_lock().push_back(BPopClient { key, tx, dir });
                    let response = rx
                        .map_ok(|bytes| {
                            RedisValue::Array(vec![
                                RedisValue::String(key_response),
                                RedisValue::String(bytes),
                            ])
                        })
                        .boxed();
                    CommandResponse::Block(response)
                }
            },
            Command::LRange { key, start, stop } => {
                let elems = storage.lrange(key, start, stop);
                RedisValue::Array(elems.into_iter().map(RedisValue::String).collect()).into()
            }
        }
    }
}
