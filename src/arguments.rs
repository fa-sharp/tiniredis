use std::collections::VecDeque;

use anyhow::{bail, Context};

use crate::parser::RedisValue;

/// The parsed command and argument strings
pub struct Arguments {
    /// Uppercase command string
    command: String,
    /// Arguments
    args: VecDeque<RedisValue>,
}

impl Arguments {
    pub fn from_raw_value(raw_value: RedisValue) -> anyhow::Result<Self> {
        let RedisValue::Array(values) = raw_value else {
            bail!("No command given")
        };
        let mut args = VecDeque::from(values);

        let Some(RedisValue::String(raw_command)) = args.pop_front() else {
            bail!("Invalid command");
        };
        let command = std::str::from_utf8(&raw_command)
            .context("Invalid command")?
            .to_ascii_uppercase();

        Ok(Self { command, args })
    }

    pub fn command(&self) -> &str {
        self.command.as_str()
    }

    /// Pop the next argument or return an error
    pub fn pop_arg(&mut self, name: &str) -> anyhow::Result<bytes::Bytes> {
        let Some(RedisValue::String(arg)) = self.args.pop_front() else {
            bail!("{}: {name} argument missing", self.command);
        };
        Ok(arg)
    }
}
