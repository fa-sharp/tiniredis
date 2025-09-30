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
        "GET" => {
            let key = args.pop("key")?;
            Command::Get { key }
        }
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
            let count = args.pop_optional_i64()?.unwrap_or(1);
            let dir = match args.command() {
                "RPOP" => ListDirection::Right,
                "LPOP" => ListDirection::Left,
                _ => unreachable!(),
            };
            Command::Pop { key, dir, count }
        }
        "BRPOP" | "BLPOP" => {
            let key = args.pop("key")?;
            let dir = match args.command() {
                "BRPOP" => ListDirection::Right,
                "BLPOP" => ListDirection::Left,
                _ => unreachable!(),
            };
            Command::BPop { key, dir }
        }
        "LLEN" => {
            let key = args.pop("key")?;
            Command::LLen { key }
        }
        "LRANGE" => {
            let key = args.pop("key")?;
            let start = args.pop_i64("start index")?;
            let stop = args.pop_i64("stop index")?;

            Command::LRange { key, start, stop }
        }
        cmd => bail!("Unrecognized command '{cmd}'"),
    };

    Ok(command)
}
