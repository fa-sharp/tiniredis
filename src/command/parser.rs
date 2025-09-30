use std::collections::VecDeque;

use anyhow::bail;

use super::Command;
use crate::{arguments::Arguments, storage::ListDirection};

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
            let ex = args.pop_optional_named("EX");
            let px = args.pop_optional_named("PX");
            let ttl = match (ex, px) {
                (None, None) => None,
                (Some(ex), None) => Some(
                    std::str::from_utf8(&ex)?
                        .parse::<u64>()
                        .map(|secs| secs * 1000)?,
                ),
                (None, Some(px)) => Some(std::str::from_utf8(&px)?.parse()?),
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
            let count = args.pop_optional_parse()?.unwrap_or(1);
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
        "LLEN" => {
            let key = args.pop("key")?;
            Command::LLen { key }
        }
        "LRANGE" => {
            let key = args.pop("key")?;
            let start = args.pop_parse("start index")?;
            let stop = args.pop_parse("stop index")?;
            Command::LRange { key, start, stop }
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
        cmd => bail!("Unrecognized command '{cmd}'"),
    };

    if args.remaining().len() != 0 {
        let mut message = String::from("Unrecognized arguments: ");
        for arg in args.remaining() {
            if let Some(str) = arg.as_bytes().and_then(|a| std::str::from_utf8(a).ok()) {
                message.push_str(str);
                message.push_str(" ");
            }
        }
        bail!("{message}");
    }

    Ok(command)
}
