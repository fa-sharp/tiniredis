use bytes_utils::Str;

use crate::{parser::RedisValue, request::Command};

pub fn process_command(command: Command) -> RedisValue {
    match command {
        Command::PING => RedisValue::SimpleString(Str::from_static("PONG")),
        Command::ECHO { message } => RedisValue::String(message),
    }
}
