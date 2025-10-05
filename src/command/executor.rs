use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, TryFutureExt};
use tinikeyval_protocol::{constants, RespValue};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::{
    server::Config,
    storage::{
        geo::GeoStorage,
        list::ListStorage,
        set::SetStorage,
        sorted_set::SortedSetStorage,
        stream::{StreamEntry, StreamStorage},
        Storage,
    },
    tasks::{Notifiers, Queues},
};

use super::{Command, CommandResponse};

/// Execute the command, and format the response into [`RespValue`] (RESP format)
pub fn execute_command(
    command: Command,
    storage: &mut (impl Storage
              + ListStorage
              + SetStorage
              + SortedSetStorage
              + StreamStorage
              + GeoStorage),
    config: &Config,
    queues: &Queues,
    notifiers: &Notifiers,
) -> Result<CommandResponse, Bytes> {
    let command_response: CommandResponse = match command {
        Command::Auth(pass) => CommandResponse::Auth(pass),
        Command::Ping => RespValue::SimpleString(Bytes::from_static(b"PONG")).into(),
        Command::Echo { message } => RespValue::String(message).into(),
        Command::DbSize => RespValue::Int(storage.size()).into(),
        Command::FlushDb => {
            let size = storage.size();
            storage.flush();
            notifiers.change_incr(size);
            constants::OK.into()
        }
        Command::ConfigGet { param } => {
            let value = match param.as_ref() {
                b"dir" => Bytes::copy_from_slice(config.rdb_dir.as_bytes()),
                b"dbfilename" => Bytes::copy_from_slice(config.rdb_filename.as_bytes()),
                _ => Err(Bytes::from("ERR unrecognized parameter"))?,
            };

            RespValue::Array(vec![RespValue::String(param), RespValue::String(value)]).into()
        }
        Command::Multi => CommandResponse::Transaction,
        Command::Exec => RespValue::Error(Bytes::from_static(b"ERR EXEC without MULTI")).into(),
        Command::Discard => {
            RespValue::Error(Bytes::from_static(b"ERR DISCARD without MULTI")).into()
        }
        Command::Get { key } => match storage.get(&key) {
            Some(val) => RespValue::String(val).into(),
            None => RespValue::NilString.into(),
        },
        Command::Set { key, val, ttl } => {
            storage.set(key, val, ttl);
            notifiers.change_incr(1);
            constants::OK.into()
        }
        Command::Type { key } => RespValue::SimpleString(storage.kind(&key)).into(),
        Command::Ttl { key } => RespValue::Int(storage.ttl(&key)).into(),
        Command::Del { keys } => {
            let mut count = 0;
            for key in keys {
                if storage.del(&key) {
                    count += 1;
                }
            }
            notifiers.change_incr(count);
            RespValue::Int(count).into()
        }
        Command::Incr { key } => {
            let incr = storage.incr(key)?;
            notifiers.change_incr(1);
            RespValue::Int(incr).into()
        }
        Command::Keys { .. } => {
            RespValue::Array(storage.keys().into_iter().map(RespValue::String).collect()).into()
        }
        Command::Push { key, elems, dir } => {
            let len = storage.push(key.clone(), elems, dir)?;
            notifiers.change_incr(1);
            notifiers.bpop_notify(key); // notify blocking POP task
            RespValue::Int(len).into()
        }
        Command::Pop { key, dir, count } => match storage.pop(&key, dir, count) {
            Some(mut elems) => {
                notifiers.change_incr(1);
                if count == 1 {
                    RespValue::String(elems.pop().expect("should have 1 item")).into()
                } else {
                    RespValue::Array(elems.into_iter().map(RespValue::String).collect()).into()
                }
            }
            None => RespValue::NilString.into(),
        },
        Command::BPop {
            key,
            dir,
            timeout_millis,
        } => {
            if let Some(mut elems) = storage.pop(&key, dir, 1) {
                notifiers.change_incr(1);
                RespValue::Array(vec![
                    RespValue::String(key),
                    RespValue::String(elems.pop().expect("should have 1 item")),
                ])
                .into()
            } else {
                let key_response = key.clone();
                let (tx, rx) = oneshot::channel();
                queues.bpop_push(key, dir, tx);
                let block_response = if timeout_millis == 0 {
                    rx.map_ok(|bytes| {
                        Ok(RespValue::Array(vec![
                            RespValue::String(key_response),
                            RespValue::String(bytes),
                        ]))
                    })
                    .boxed()
                } else {
                    tokio::time::timeout(Duration::from_millis(timeout_millis), rx)
                        .map(|res| match res {
                            Ok(Ok(bytes)) => Ok(Ok(RespValue::Array(vec![
                                RespValue::String(key_response), // POP response
                                RespValue::String(bytes),
                            ]))),
                            Ok(Err(e)) => Err(e), // Receiver disconnected
                            Err(_) => Ok(Ok(RespValue::NilArray)), // Timeout
                        })
                        .boxed()
                };
                CommandResponse::Block(block_response)
            }
        }
        Command::LLen { key } => RespValue::Int(storage.llen(&key)).into(),
        Command::LRange { key, start, stop } => {
            let elems = storage.lrange(&key, start, stop);
            RespValue::Array(elems.into_iter().map(RespValue::String).collect()).into()
        }
        Command::SAdd { key, members } => {
            let num = storage.sadd(key, members)?;
            if num > 0 {
                notifiers.change_incr(1);
            }
            RespValue::Int(num).into()
        }
        Command::SRem { key, members } => {
            let num = storage.srem(&key, members)?;
            if num > 0 {
                notifiers.change_incr(1);
            }
            RespValue::Int(num).into()
        }
        Command::SCard { key } => RespValue::Int(storage.scard(&key)?).into(),
        Command::SMembers { key } => {
            let members = storage.smembers(&key)?;
            RespValue::Array(members.into_iter().map(RespValue::String).collect()).into()
        }
        Command::SIsMember { key, member } => match storage.sismember(&key, &member)? {
            true => RespValue::Int(1).into(),
            false => RespValue::Int(0).into(),
        },
        Command::ZAdd { key, members } => {
            let num = storage.zadd(key, members)?;
            if num > 0 {
                notifiers.change_incr(1);
            }
            RespValue::Int(num).into()
        }
        Command::ZRank { key, member } => match storage.zrank(&key, member)? {
            Some(rank) => RespValue::Int(rank).into(),
            None => RespValue::NilString.into(),
        },
        Command::ZScore { key, member } => match storage.zscore(&key, &member)? {
            Some(score) => RespValue::String(Bytes::from(score.to_string())).into(),
            None => RespValue::NilString.into(),
        },
        Command::ZCard { key } => RespValue::Int(storage.zcard(&key)?).into(),
        Command::ZRange { key, start, stop } => {
            let members = storage.zrange(&key, start, stop)?;
            RespValue::Array(members.into_iter().map(RespValue::String).collect()).into()
        }
        Command::ZRem { key, members } => {
            let num = storage.zrem(&key, members)?;
            if num > 0 {
                notifiers.change_incr(1);
            }
            RespValue::Int(num).into()
        }
        Command::GeoAdd { key, members } => {
            let num = storage.geoadd(key, members)?;
            if num > 0 {
                notifiers.change_incr(1);
            }
            RespValue::Int(num).into()
        }
        Command::GeoPos { key, members } => {
            let member_coords = storage.geopos(&key, members)?;
            let values = member_coords
                .into_iter()
                .map(|coord| match coord {
                    Some((lon, lat)) => RespValue::Array(vec![
                        RespValue::String(Bytes::from(lon.to_string())),
                        RespValue::String(Bytes::from(lat.to_string())),
                    ]),
                    None => RespValue::NilArray,
                })
                .collect();
            RespValue::Array(values).into()
        }
        Command::GeoDist {
            key,
            member1,
            member2,
        } => match storage.geodist(&key, &member1, &member2)? {
            Some(dist) => RespValue::String(Bytes::from(dist.to_string())).into(),
            None => RespValue::NilString.into(),
        },
        Command::GeoSearch { key, from, radius } => {
            let members = storage.geosearch(&key, from, radius)?;
            RespValue::Array(members.into_iter().map(RespValue::String).collect()).into()
        }
        Command::XAdd { key, id, data } => {
            let id = storage.xadd(key.clone(), id, data)?;
            notifiers.change_incr(1);
            notifiers.xread_notify(key); // notify blocking XREAD task
            RespValue::String(format_stream_id(id)).into()
        }
        Command::XLen { key } => RespValue::Int(storage.xlen(&key)).into(),
        Command::XRange { key, start, end } => {
            let entries = storage.xrange(&key, &start, &end)?;
            RespValue::Array(entries.into_iter().map(format_stream_entry).collect()).into()
        }
        Command::XRead { streams, block } => {
            let (parsed_streams, response) = storage.xread(streams.clone())?;
            if !response.is_empty() {
                RespValue::Array(response.into_iter().map(format_stream).collect()).into()
            } else if let Some(block_millis) = block {
                let (tx, rx) = oneshot::channel();
                queues.xread_push(
                    parsed_streams
                        .into_iter()
                        .map(|(key, id)| (key, format_stream_id(id)))
                        .collect(),
                    tx,
                );
                let block_response = if block_millis == 0 {
                    rx.map_ok(|res| {
                        res.map(|streams| {
                            let resp_format = streams.into_iter().map(format_stream).collect();
                            RespValue::Array(resp_format)
                        })
                    })
                    .boxed()
                } else {
                    tokio::time::timeout(Duration::from_millis(block_millis), rx)
                        .map(|res| match res {
                            Ok(Ok(res)) => Ok(res.map(|streams| {
                                let resp_format = streams.into_iter().map(format_stream).collect();
                                RespValue::Array(resp_format) // XREAD response
                            })),
                            Ok(Err(recv_err)) => Err(recv_err), // Receiver disconnected
                            Err(_) => Ok(Ok(RespValue::NilArray)), // Timeout
                        })
                        .boxed()
                };
                CommandResponse::Block(block_response)
            } else {
                RespValue::NilArray.into()
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
            Ok(rx) => CommandResponse::Block(rx.map_ok(|count| Ok(RespValue::Int(count))).boxed()),
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

fn format_stream_entry((id, data): StreamEntry) -> RespValue {
    RespValue::Array(vec![
        RespValue::String(format_stream_id(id)),
        RespValue::Array(
            data.into_iter()
                .flat_map(|(field, value)| [RespValue::String(field), RespValue::String(value)])
                .collect(),
        ),
    ])
}

fn format_stream((key, entries): (Bytes, Vec<StreamEntry>)) -> RespValue {
    RespValue::Array(vec![
        RespValue::String(key),
        RespValue::Array(entries.into_iter().map(format_stream_entry).collect()),
    ])
}
