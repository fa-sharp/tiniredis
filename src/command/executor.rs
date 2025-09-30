use bytes::Bytes;
use futures::{FutureExt, TryFutureExt};
use tokio::sync::oneshot;

use super::{Command, CommandResponse};
use crate::{
    parser::RedisValue, queues::Queues, senders::Senders, storage::Storage, tasks::BPopClient,
};

pub fn execute_command(
    command: Command,
    storage: &mut impl Storage,
    queues: &Queues,
    senders: &Senders,
) -> CommandResponse {
    match command {
        Command::Ping => RedisValue::SimpleString(Bytes::from_static(b"PONG")).into(),
        Command::Echo { message } => RedisValue::String(message).into(),
        Command::DbSize => RedisValue::Int(storage.size()).into(),
        Command::FlushDb => {
            storage.flush();
            RedisValue::SimpleString(Bytes::from_static(b"OK")).into()
        }
        Command::Get { key } => match storage.get(&key) {
            Some(val) => RedisValue::String(val).into(),
            None => RedisValue::NilString.into(),
        },
        Command::Set { key, val, ttl } => {
            storage.set(key, val, ttl);
            RedisValue::SimpleString(Bytes::from_static(b"OK")).into()
        }
        Command::Push { key, elems, dir } => match storage.push(key.clone(), elems, dir) {
            Ok(len) => {
                senders.notify_bpop(key); // notify blocking pop clients
                RedisValue::Int(len).into()
            }
            Err(bytes) => RedisValue::Error(bytes).into(),
        },
        Command::Pop { key, dir, count } => match storage.pop(&key, dir, count) {
            Some(mut elems) => {
                if count == 1 {
                    RedisValue::String(elems.pop().expect("should have 1 item")).into()
                } else {
                    RedisValue::Array(elems.into_iter().map(RedisValue::String).collect()).into()
                }
            }
            None => RedisValue::NilString.into(),
        },
        Command::BPop { key, dir } => match storage.pop(&key, dir, 1) {
            Some(mut elems) => RedisValue::Array(vec![
                RedisValue::String(key),
                RedisValue::String(elems.pop().expect("should have 1 item")),
            ])
            .into(),
            None => {
                let (tx, rx) = oneshot::channel();
                let key_response = key.clone();
                queues.bpop_lock().push_back(BPopClient { key, tx, dir });
                let response = rx
                    .map_ok(|bytes| {
                        RedisValue::Array(vec![
                            RedisValue::String(key_response),
                            RedisValue::String(bytes),
                        ])
                    })
                    .boxed();
                CommandResponse::Block(response)
            }
        },
        Command::LLen { key } => RedisValue::Int(storage.llen(&key)).into(),
        Command::LRange { key, start, stop } => {
            let elems = storage.lrange(&key, start, stop);
            RedisValue::Array(elems.into_iter().map(RedisValue::String).collect()).into()
        }
    }
}
