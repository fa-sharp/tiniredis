use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, TryFutureExt};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use super::{Command, CommandResponse};
use crate::{
    notifiers::Notifiers,
    protocol::RedisValue,
    queues::Queues,
    storage::{
        list::ListStorage,
        set::SetStorage,
        sorted_set::SortedSetStorage,
        stream::{StreamEntry, StreamStorage},
        Storage,
    },
    tasks::{BPopClient, XReadClient},
};

/// Execute the command, and format the response into [`RedisValue`] (RESP format)
pub fn execute_command(
    command: Command,
    storage: &mut (impl Storage + ListStorage + SetStorage + SortedSetStorage + StreamStorage),
    queues: &Queues,
    notifiers: &Notifiers,
) -> Result<CommandResponse, Bytes> {
    let command_response: CommandResponse = match command {
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
        Command::Type { key } => RedisValue::SimpleString(storage.kind(&key)).into(),
        Command::Ttl { key } => RedisValue::Int(storage.ttl(&key)).into(),
        Command::Del { keys } => {
            let mut count = 0;
            for key in keys {
                if storage.del(&key) {
                    count += 1;
                }
            }
            RedisValue::Int(count).into()
        }
        Command::Incr { key } => RedisValue::Int(storage.incr(key)?).into(),
        Command::Push { key, elems, dir } => {
            let len = storage.push(key.clone(), elems, dir)?;
            notifiers.bpop_notify(key); // notify blocking POP task
            RedisValue::Int(len).into()
        }
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
        Command::BPop {
            key,
            dir,
            timeout_millis,
        } => {
            if let Some(mut elems) = storage.pop(&key, dir, 1) {
                RedisValue::Array(vec![
                    RedisValue::String(key),
                    RedisValue::String(elems.pop().expect("should have 1 item")),
                ])
                .into()
            } else {
                let key_response = key.clone();
                let (tx, rx) = oneshot::channel();
                queues.bpop_push(BPopClient { key, dir, tx });
                let block_response = if timeout_millis == 0 {
                    rx.map_ok(|bytes| {
                        Ok(RedisValue::Array(vec![
                            RedisValue::String(key_response),
                            RedisValue::String(bytes),
                        ]))
                    })
                    .boxed()
                } else {
                    tokio::time::timeout(Duration::from_millis(timeout_millis), rx)
                        .map(|res| match res {
                            Ok(Ok(bytes)) => Ok(Ok(RedisValue::Array(vec![
                                RedisValue::String(key_response), // POP response
                                RedisValue::String(bytes),
                            ]))),
                            Ok(Err(e)) => Err(e), // Receiver disconnected
                            Err(_) => Ok(Ok(RedisValue::NilArray)), // Timeout
                        })
                        .boxed()
                };
                CommandResponse::Block(block_response)
            }
        }
        Command::LLen { key } => RedisValue::Int(storage.llen(&key)).into(),
        Command::LRange { key, start, stop } => {
            let elems = storage.lrange(&key, start, stop);
            RedisValue::Array(elems.into_iter().map(RedisValue::String).collect()).into()
        }
        Command::SAdd { key, members } => RedisValue::Int(storage.sadd(key, members)?).into(),
        Command::SRem { key, members } => RedisValue::Int(storage.srem(&key, members)?).into(),
        Command::SCard { key } => RedisValue::Int(storage.scard(&key)?).into(),
        Command::SMembers { key } => {
            let members = storage.smembers(&key)?;
            RedisValue::Array(members.into_iter().map(RedisValue::String).collect()).into()
        }
        Command::SIsMember { key, member } => match storage.sismember(&key, &member)? {
            true => RedisValue::Int(1).into(),
            false => RedisValue::Int(0).into(),
        },
        Command::ZAdd { key, members } => RedisValue::Int(storage.zadd(key, members)?).into(),
        Command::ZRank { key, member } => match storage.zrank(&key, member)? {
            Some(rank) => RedisValue::Int(rank).into(),
            None => RedisValue::NilString.into(),
        },
        Command::ZScore { key, member } => match storage.zscore(&key, &member)? {
            Some(score) => RedisValue::String(Bytes::from(score.to_string())).into(),
            None => RedisValue::NilString.into(),
        },
        Command::ZCard { key } => RedisValue::Int(storage.zcard(&key)?).into(),
        Command::ZRange { key, start, stop } => {
            let members = storage.zrange(&key, start, stop)?;
            RedisValue::Array(members.into_iter().map(RedisValue::String).collect()).into()
        }
        Command::ZRem { key, members } => RedisValue::Int(storage.zrem(&key, members)?).into(),
        Command::XAdd { key, id, data } => {
            let id = storage.xadd(key.clone(), id, data)?;
            notifiers.xread_notify(key); // notify blocking XREAD task
            RedisValue::String(format_stream_id(id)).into()
        }
        Command::XLen { key } => RedisValue::Int(storage.xlen(&key)).into(),
        Command::XRange { key, start, end } => {
            let entries = storage.xrange(&key, &start, &end)?;
            RedisValue::Array(entries.into_iter().map(format_stream_entry).collect()).into()
        }
        Command::XRead { streams, block } => {
            let (parsed_streams, response) = storage.xread(streams.clone())?;
            if !response.is_empty() {
                RedisValue::Array(response.into_iter().map(format_stream).collect()).into()
            } else if let Some(block_millis) = block {
                let (tx, rx) = oneshot::channel();
                queues.xread_push(XReadClient {
                    streams: parsed_streams
                        .into_iter()
                        .map(|(key, id)| (key, format_stream_id(id)))
                        .collect(),
                    tx: Some(tx),
                });
                let block_response = if block_millis == 0 {
                    rx.map_ok(|res| {
                        res.map(|streams| {
                            let resp_format = streams.into_iter().map(format_stream).collect();
                            RedisValue::Array(resp_format)
                        })
                    })
                    .boxed()
                } else {
                    tokio::time::timeout(Duration::from_millis(block_millis), rx)
                        .map(|res| match res {
                            Ok(Ok(res)) => Ok(res.map(|streams| {
                                let resp_format = streams.into_iter().map(format_stream).collect();
                                RedisValue::Array(resp_format) // XREAD response
                            })),
                            Ok(Err(recv_err)) => Err(recv_err), // Receiver disconnected
                            Err(_) => Ok(Ok(RedisValue::NilArray)), // Timeout
                        })
                        .boxed()
                };
                CommandResponse::Block(block_response)
            } else {
                RedisValue::NilArray.into()
            }
        }
        Command::Subscribe { channels } => {
            let (tx, rx) = mpsc::unbounded_channel();
            let client_id = queues.pubsub_add(tx);
            match notifiers.pubsub_subscribe(client_id, channels) {
                Ok(_) => CommandResponse::Subscribed(client_id, rx),
                Err(err) => {
                    warn!("dropped pubsub receiver: {err}");
                    Err(Bytes::from_static(b"Failed to subscribe"))?
                }
            }
        }
        Command::Publish { channel, message } => match notifiers.pubsub_publish(channel, message) {
            Ok(rx) => CommandResponse::Block(rx.map_ok(|count| Ok(RedisValue::Int(count))).boxed()),
            Err(err) => {
                warn!("dropped pubsub receiver: {err}");
                Err(Bytes::from_static(b"Failed to send message"))?
            }
        },
    };

    Ok(command_response)
}

fn format_stream_id((ms, seq): (u64, u64)) -> Bytes {
    let (ms_str, seq_str) = (ms.to_string(), seq.to_string());
    let mut bytes = BytesMut::with_capacity(ms_str.len() + seq_str.len() + 1);
    bytes.put_slice(ms_str.as_bytes());
    bytes.put_u8(b'-');
    bytes.put_slice(seq_str.as_bytes());
    bytes.freeze()
}

fn format_stream_entry((id, data): StreamEntry) -> RedisValue {
    RedisValue::Array(vec![
        RedisValue::String(format_stream_id(id)),
        RedisValue::Array(
            data.into_iter()
                .flat_map(|(field, value)| [RedisValue::String(field), RedisValue::String(value)])
                .collect(),
        ),
    ])
}

fn format_stream((key, entries): (Bytes, Vec<StreamEntry>)) -> RedisValue {
    RedisValue::Array(vec![
        RedisValue::String(key),
        RedisValue::Array(entries.into_iter().map(format_stream_entry).collect()),
    ])
}
