use std::collections::VecDeque;

use anyhow::bail;

use super::Command;
use crate::{arguments::Arguments, storage::list::ListDirection};

pub fn parse_command(mut args: Arguments) -> anyhow::Result<Command> {
    let command = match args.command() {
        "PING" => Command::Ping,
        "DBSIZE" => Command::DbSize,
        "FLUSHDB" => Command::FlushDb,
        "ECHO" => {
            let message = args.pop("message")?;
            Command::Echo { message }
        }
        "GET" => Command::Get {
            key: args.pop("key")?,
        },
        "SET" => {
            let key = args.pop("key")?;
            let val = args.pop("value")?;
            let ex: Option<u64> = args.pop_parse_optional_named("EX")?;
            let px: Option<u64> = args.pop_parse_optional_named("PX")?;
            let ttl = match (ex, px) {
                (None, None) => None,
                (Some(ex), None) => Some(ex * 1000),
                (None, Some(px)) => Some(px),
                (Some(_), Some(_)) => bail!("can't provide both EX and PX"),
            };
            Command::Set { key, val, ttl }
        }
        "TYPE" => Command::Type {
            key: args.pop("key")?,
        },
        "TTL" => Command::Ttl {
            key: args.pop("key")?,
        },
        "DEL" => {
            let mut keys = vec![args.pop("key")?];
            while let Some(key) = args.pop_optional() {
                keys.push(key);
            }
            Command::Del { keys }
        }
        "INCR" => Command::Incr {
            key: args.pop("key")?,
        },
        "RPUSH" | "LPUSH" => {
            let key = args.pop("key")?;
            let elem = args.pop("element")?;
            let mut elems: VecDeque<_> = vec![elem].into();
            while let Some(elem) = args.pop_optional() {
                elems.push_back(elem);
            }
            let dir = match args.command() {
                "RPUSH" => ListDirection::Right,
                "LPUSH" => ListDirection::Left,
                _ => unreachable!(),
            };
            Command::Push { key, elems, dir }
        }
        "RPOP" | "LPOP" => {
            let key = args.pop("key")?;
            let count = args.pop_parse_optional()?.unwrap_or(1);
            let dir = match args.command() {
                "RPOP" => ListDirection::Right,
                "LPOP" => ListDirection::Left,
                _ => unreachable!(),
            };
            Command::Pop { key, dir, count }
        }
        "BRPOP" | "BLPOP" => {
            let key = args.pop("key")?;
            let timeout = args.pop_parse::<f32>("timeout")?;
            let timeout_millis = (timeout * 1000.0).round() as u64;
            let dir = match args.command() {
                "BRPOP" => ListDirection::Right,
                "BLPOP" => ListDirection::Left,
                _ => unreachable!(),
            };
            Command::BPop {
                key,
                dir,
                timeout_millis,
            }
        }
        "LLEN" => Command::LLen {
            key: args.pop("key")?,
        },
        "LRANGE" => {
            let key = args.pop("key")?;
            let start = args.pop_parse("start index")?;
            let stop = args.pop_parse("stop index")?;
            Command::LRange { key, start, stop }
        }
        "SADD" | "SREM" => {
            let key = args.pop("key")?;
            let mut members = vec![args.pop("member")?];
            while let Some(member) = args.pop_optional() {
                members.push(member);
            }
            match args.command() {
                "SADD" => Command::SAdd { key, members },
                "SREM" => Command::SRem { key, members },
                _ => unreachable!(),
            }
        }
        "SCARD" => Command::SCard {
            key: args.pop("key")?,
        },
        "SMEMBERS" => Command::SMembers {
            key: args.pop("key")?,
        },
        "SISMEMBER" => {
            let key = args.pop("key")?;
            let member = args.pop("member")?;
            Command::SIsMember { key, member }
        }
        "ZADD" => {
            let key = args.pop("key")?;
            let mut members = vec![(args.pop_parse("score")?, args.pop("member")?)];
            while let (Some(score), Some(member)) =
                (args.pop_parse_optional()?, args.pop_optional())
            {
                members.push((score, member));
            }
            Command::ZAdd { key, members }
        }
        "ZRANK" => {
            let key = args.pop("key")?;
            let member = args.pop("member")?;
            Command::ZRank { key, member }
        }
        "ZSCORE" => {
            let key = args.pop("key")?;
            let member = args.pop("member")?;
            Command::ZScore { key, member }
        }
        "ZCARD" => Command::ZCard {
            key: args.pop("key")?,
        },
        "ZRANGE" => {
            let key = args.pop("key")?;
            let start = args.pop_parse("start index")?;
            let stop = args.pop_parse("stop index")?;
            Command::ZRange { key, start, stop }
        }
        "ZREM" => {
            let key = args.pop("key")?;
            let mut members = vec![args.pop("member")?];
            while let Some(member) = args.pop_optional() {
                members.push(member);
            }
            Command::ZRem { key, members }
        }
        "XADD" => {
            let key = args.pop("key")?;
            let id = args.pop("id")?;
            let mut data = vec![(args.pop("field")?, args.pop("value")?)];
            while let (Some(field), Some(value)) = (args.pop_optional(), args.pop_optional()) {
                data.push((field, value));
            }
            Command::XAdd { key, id, data }
        }
        "XLEN" => Command::XLen {
            key: args.pop("key")?,
        },
        "XRANGE" => {
            let key = args.pop("key")?;
            let start = args.pop("start")?;
            let end = args.pop("end")?;
            Command::XRange { key, start, end }
        }
        "XREAD" => {
            let block = args.pop_parse_optional_named("block")?;
            match args.pop("streams")?.to_ascii_uppercase().as_slice() {
                b"STREAMS" => {}
                _ => bail!("STREAMS keyword is required"),
            }

            let mut keys_and_ids = Vec::new();
            while let Some(arg) = args.pop_optional() {
                keys_and_ids.push(arg);
            }
            if keys_and_ids.len() < 2 {
                bail!("Must provide a stream key and ID");
            }
            if keys_and_ids.len() % 2 != 0 {
                bail!("Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.")
            }
            let (keys, ids) = keys_and_ids.split_at(keys_and_ids.len() / 2);
            let streams = keys.iter().cloned().zip(ids.iter().cloned()).collect();

            Command::XRead { streams, block }
        }
        "SUBSCRIBE" => {
            let mut channels = vec![args.pop("channel")?];
            while let Some(channel) = args.pop_optional() {
                channels.push(channel);
            }
            Command::Subscribe { channels }
        }
        "PUBLISH" => {
            let channel = args.pop("channel")?;
            let message = args.pop("message")?;
            Command::Publish { channel, message }
        }
        cmd => bail!("Unrecognized command '{cmd}'"),
    };

    if !args.remaining().is_empty() {
        let mut message = String::from("Unrecognized arguments: ");
        for arg in args.remaining() {
            if let Some(str) = arg.as_bytes().and_then(|a| std::str::from_utf8(a).ok()) {
                message.push_str(str);
                message.push(' ');
            }
        }
        bail!("{message}");
    }

    Ok(command)
}
