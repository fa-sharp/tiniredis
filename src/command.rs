use std::collections::VecDeque;

use anyhow::{bail, Context};
use bytes::Bytes;

use crate::{parser::RedisValue, storage::Storage};

pub enum Command {
    Ping,
    Echo { message: Bytes },
    Get { key: Bytes },
    Set { key: Bytes, val: Bytes },
}

impl Command {
    /// Parse the command from the raw input value
    pub fn from_value(raw_value: RedisValue) -> anyhow::Result<Self> {
        let RedisValue::Array(values) = raw_value else {
            bail!("No command")
        };
        let mut values = VecDeque::from(values);

        let Some(RedisValue::String(raw_command)) = values.pop_front() else {
            bail!("Invalid command");
        };
        let command_str = std::str::from_utf8(&raw_command)
            .context("Invalid command")?
            .to_ascii_uppercase();

        Ok(match command_str.as_str() {
            "PING" => Self::Ping,
            "ECHO" => {
                let Some(RedisValue::String(message)) = values.pop_front() else {
                    bail!("ECHO argument missing");
                };
                Self::Echo { message }
            }
            "GET" => {
                let Some(RedisValue::String(key)) = values.pop_front() else {
                    bail!("GET key argument missing");
                };
                Self::Get { key }
            }
            "SET" => {
                let Some(RedisValue::String(key)) = values.pop_front() else {
                    bail!("SET key argument missing");
                };
                let Some(RedisValue::String(val)) = values.pop_front() else {
                    bail!("SET value argument missing");
                };
                Self::Set { key, val }
            }
            _ => bail!("Unrecognized command"),
        })
    }

    /// Execute the command and get the response value
    pub fn execute(self, storage: &mut impl Storage) -> RedisValue {
        match self {
            Command::Ping => RedisValue::SimpleString(Bytes::from_static(b"PONG")),
            Command::Echo { message } => RedisValue::String(message.into()),
            Command::Get { key } => match storage.get(&key) {
                Some(val) => RedisValue::String(val),
                None => RedisValue::NulString,
            },
            Command::Set { key, val } => {
                storage.set(key, val);
                RedisValue::SimpleString(Bytes::from_static(b"OK"))
            }
        }
    }
}
