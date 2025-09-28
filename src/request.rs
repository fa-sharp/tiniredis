use std::{collections::VecDeque, ops::Deref};

use anyhow::bail;
use bytes_utils::Str;

use crate::parser::RedisValue;

pub enum Command {
    PING,
    ECHO { message: Str },
}

pub fn parse_command(raw_value: RedisValue) -> anyhow::Result<Command> {
    let RedisValue::Array(values) = raw_value else {
        bail!("No command")
    };
    let mut values = VecDeque::from(values);
    let Some(RedisValue::String(raw_command)) = values.pop_front() else {
        bail!("Invalid command");
    };

    Ok(match raw_command.deref().to_ascii_uppercase().as_str() {
        "PING" => Command::PING,
        "ECHO" => {
            let Some(RedisValue::String(message)) = values.pop_front() else {
                bail!("ECHO argument missing");
            };
            Command::ECHO { message }
        }
        _ => bail!("Unrecognized command"),
    })
}
