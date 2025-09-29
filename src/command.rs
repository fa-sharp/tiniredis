use anyhow::bail;
use bytes::Bytes;

use crate::{arguments::Arguments, parser::RedisValue, storage::Storage};

pub enum Command {
    Ping,
    Echo { message: Bytes },
    Get { key: Bytes },
    Set { key: Bytes, val: Bytes },
}

impl Command {
    /// Parse the command from the raw input value
    pub fn from_value(raw_value: RedisValue) -> anyhow::Result<Self> {
        let mut args = Arguments::from_raw_value(raw_value)?;

        Ok(match args.command() {
            "PING" => Self::Ping,
            "ECHO" => {
                let message = args.pop_arg("message")?;
                Self::Echo { message }
            }
            "GET" => {
                let key = args.pop_arg("key")?;
                Self::Get { key }
            }
            "SET" => {
                let key = args.pop_arg("key")?;
                let val = args.pop_arg("value")?;
                Self::Set { key, val }
            }
            _ => bail!("Unrecognized command"),
        })
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
            Command::Set { key, val } => {
                storage.set(key, val);
                RedisValue::SimpleString(Bytes::from_static(b"OK"))
            }
        }
    }
}
