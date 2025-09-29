use std::collections::VecDeque;

use anyhow::{bail, Context};
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
    pub fn pop(&mut self, name: &str) -> anyhow::Result<Bytes> {
        let Some(arg) = self.args.pop_front().and_then(|a| a.into_bytes()) else {
            bail!("{}: {name} argument missing", self.command);
        };
        Ok(arg)
    }

    /// Pop and parse the next argument as an i64 or return an error
    pub fn pop_i64(&mut self, name: &str) -> anyhow::Result<i64> {
        let Some(arg) = self.args.pop_front().and_then(|a| a.into_bytes()) else {
            bail!("{}: {name} argument missing", self.command);
        };
        Ok(std::str::from_utf8(&arg)
            .context("invalid integer")?
            .parse()
            .context("invalid integer")?)
    }

    /// Pop the next argument if it exists
    pub fn pop_optional(&mut self) -> Option<Bytes> {
        self.args.pop_front().and_then(|a| a.into_bytes())
    }

    /// Get optional named argument (e.g. if `EX 123` given for SET, get `123`)
    pub fn pop_optional_named(&mut self, name: &str) -> Option<Bytes> {
        if let Some(arg_idx) = self.args.iter().position(|a| {
            if let Some(name_arg) = a.as_bytes() {
                return name_arg.eq_ignore_ascii_case(name.as_bytes());
            }
            false
        }) {
            if self.args.get(arg_idx + 1).is_some() {
                self.args.remove(arg_idx);
                return self.args.remove(arg_idx).and_then(|a| a.into_bytes());
            }
        }
        None
    }
}
