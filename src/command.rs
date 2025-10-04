use std::collections::VecDeque;

use bytes::Bytes;
use futures::future::BoxFuture;
use tinikeyval_protocol::RedisValue;
use tokio::sync::{mpsc, oneshot};

use crate::{
    arguments::Arguments,
    notifiers::Notifiers,
    queues::Queues,
    storage::{
        geo::GeoStorage,
        list::{ListDirection, ListStorage},
        set::SetStorage,
        sorted_set::SortedSetStorage,
        stream::StreamStorage,
        Storage,
    },
};

mod executor;
pub mod parser;

/// Represents a parsed Redis command
#[derive(Debug)]
pub enum Command {
    Ping,
    DbSize,
    FlushDb,
    Multi,
    Exec,
    Discard,
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
    Incr {
        key: Bytes,
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
    SAdd {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SRem {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SCard {
        key: Bytes,
    },
    SMembers {
        key: Bytes,
    },
    SIsMember {
        key: Bytes,
        member: Bytes,
    },
    ZAdd {
        key: Bytes,
        members: Vec<(f64, Bytes)>,
    },
    ZRank {
        key: Bytes,
        member: Bytes,
    },
    ZScore {
        key: Bytes,
        member: Bytes,
    },
    ZRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    ZCard {
        key: Bytes,
    },
    ZRem {
        key: Bytes,
        members: Vec<Bytes>,
    },
    GeoAdd {
        key: Bytes,
        members: Vec<((f64, f64), Bytes)>,
    },
    GeoPos {
        key: Bytes,
        members: Vec<Bytes>,
    },
    GeoDist {
        key: Bytes,
        member1: Bytes,
        member2: Bytes,
    },
    GeoSearch {
        key: Bytes,
        from: (f64, f64),
        radius: f64,
    },
    XAdd {
        key: Bytes,
        id: Bytes,
        data: Vec<(Bytes, Bytes)>,
    },
    XLen {
        key: Bytes,
    },
    XRange {
        key: Bytes,
        start: Bytes,
        end: Bytes,
    },
    XRead {
        streams: Vec<(Bytes, Bytes)>,
        block: Option<u64>,
    },
    Subscribe {
        channels: Vec<Bytes>,
    },
    Publish {
        channel: Bytes,
        message: Bytes,
    },
}

/// The possible responses from a command
pub enum CommandResponse {
    /// An immediate response value
    Value(RedisValue),
    /// A blocking response
    Block(BoxFuture<'static, Result<Result<RedisValue, Bytes>, oneshot::error::RecvError>>),
    /// Subscribed to pubsub
    Subscribed(u64, mpsc::UnboundedReceiver<RedisValue>),
    /// Enter a MULTI block
    Transaction,
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
        storage: &mut (impl Storage
                  + ListStorage
                  + SetStorage
                  + SortedSetStorage
                  + StreamStorage
                  + GeoStorage),
        queues: &Queues,
        notifiers: &Notifiers,
    ) -> Result<CommandResponse, Bytes> {
        executor::execute_command(self, storage, queues, notifiers)
    }
}
