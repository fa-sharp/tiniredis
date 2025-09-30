use std::collections::VecDeque;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::sync::oneshot;

use crate::{
    arguments::Arguments,
    parser::RedisValue,
    queues::Queues,
    senders::Senders,
    storage::{ListDirection, Storage},
};

mod executor;
mod parser;

/// Represents a parsed Redis command
#[derive(Debug)]
pub enum Command {
    Ping,
    DbSize,
    FlushDb,
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
    Type {
        key: Bytes,
    },
    Ttl {
        key: Bytes,
    },
    Del {
        keys: Vec<Bytes>,
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
        timeout_millis: u64,
    },
    LLen {
        key: Bytes,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    XAdd {
        key: Bytes,
        id: (u64, u64),
        data: Vec<(Bytes, Bytes)>,
    },
}

/// The possible responses from a command
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
        let args = Arguments::from_raw_value(raw_value)?;
        let command = parser::parse_command(args)?;
        Ok(command)
    }

    /// Execute the command and get the response
    pub fn execute(
        self,
        storage: &mut impl Storage,
        queues: &Queues,
        senders: &Senders,
    ) -> CommandResponse {
        executor::execute_command(self, storage, queues, senders)
    }
}
