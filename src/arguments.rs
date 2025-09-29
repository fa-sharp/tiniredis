use std::collections::VecDeque;

use anyhow::bail;
use bytes::Bytes;

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
        let command = args
            .pop_front()
            .and_then(|arg| arg.into_bytes())
            .and_then(|arg_b| {
                let command_str = std::str::from_utf8(&arg_b).ok();
                command_str.map(|c| c.to_ascii_uppercase())
            })
            .ok_or_else(|| anyhow::anyhow!("Invalid command"))?;

        Ok(Self { command, args })
    }

    pub fn command(&self) -> &str {
        self.command.as_str()
    }

    /// Pop the next argument or return an error
    pub fn pop_arg(&mut self, name: &str) -> anyhow::Result<Bytes> {
        let Some(RedisValue::String(arg)) = self.args.pop_front() else {
            bail!("{}: {name} argument missing", self.command);
        };
        Ok(arg)
    }

    /// Get optional named argument (e.g. if `EX 123` given for SET, get `123`)
    pub fn optional_named_arg(&self, name: &str) -> Option<&Bytes> {
        if let Some(arg_idx) = self.args.iter().position(|a| {
            if let Some(name_arg) = a.as_bytes() {
                return name_arg.eq_ignore_ascii_case(name.as_bytes());
            }
            false
        }) {
            self.args.get(arg_idx + 1).and_then(|a| a.as_bytes())
        } else {
            None
        }
    }
}
