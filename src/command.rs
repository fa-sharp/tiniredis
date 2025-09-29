use anyhow::bail;
use bytes::Bytes;

use crate::{arguments::Arguments, parser::RedisValue, storage::Storage};

#[derive(Debug)]
pub enum Command {
    Ping,
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
    RPush {
        key: Bytes,
        elem: Bytes,
        elems: Vec<Bytes>,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
}

impl Command {
    /// Parse the command from the raw input value
    pub fn from_value(raw_value: RedisValue) -> anyhow::Result<Self> {
        let mut args = Arguments::from_raw_value(raw_value)?;

        let command = match args.command() {
            "PING" => Self::Ping,
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
            "RPUSH" => {
                let key = args.pop("key")?;
                let elem = args.pop("element")?;

                let mut elems = Vec::new();
                while let Some(elem) = args.pop_optional() {
                    elems.push(elem);
                }

                Self::RPush { key, elem, elems }
            }
            "LRANGE" => {
                let key = args.pop("key")?;
                let start = args.pop_i64("start index")?;
                let stop = args.pop_i64("stop index")?;
                Self::LRange { key, start, stop }
            }
            _ => bail!("Unrecognized command"),
        };

        Ok(command)
    }

    /// Execute the command and get the response value
    pub fn execute(self, storage: &mut impl Storage) -> RedisValue {
        match self {
            Command::Ping => RedisValue::SimpleString(Bytes::from_static(b"PONG")),
            Command::Echo { message } => RedisValue::String(message),
            Command::Get { key } => match storage.get(&key) {
                Some(val) => RedisValue::String(val),
                None => RedisValue::NilString,
            },
            Command::Set { key, val, ttl } => {
                storage.set(key, val, ttl);
                RedisValue::SimpleString(Bytes::from_static(b"OK"))
            }
            Command::RPush { key, elem, elems } => match storage.rpush(key, elem, elems) {
                Ok(len) => RedisValue::Int(len),
                Err(bytes) => RedisValue::Error(bytes),
            },
            Command::LRange { key, start, stop } => {
                let elems = storage.lrange(key, start, stop);
                RedisValue::Array(elems.into_iter().map(RedisValue::String).collect())
            }
        }
    }
}
