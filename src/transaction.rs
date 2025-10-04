use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tinikeyval_protocol::{constants, RespCodec, RespValue};
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::debug;

use crate::{
    arguments::Arguments,
    command::{parser::parse_command, Command},
};

/// Process a transaction entered in a `MULTI` command, and get the queued list of commands
#[tracing::instrument(skip(cxn))]
pub async fn process_transaction(
    cxn: &mut Framed<impl AsyncWrite + AsyncBufRead + Unpin, RespCodec>,
) -> Option<Vec<Command>> {
    let mut command_queue: Vec<Command> = Vec::new();
    loop {
        let raw_command = match cxn.next().await {
            Some(Ok(raw_command)) => raw_command,
            Some(Err(_)) | None => {
                debug!("exiting transaction due to read error / disconnected");
                break;
            }
        };
        debug!("raw command: {raw_command:?}");

        let response = match Arguments::from_raw_value(raw_command) {
            Ok(args) => match args.command() {
                "EXEC" => return Some(command_queue),
                "DISCARD" => {
                    cxn.send(constants::OK).await.ok();
                    break;
                }
                _ => match parse_command(args) {
                    Ok(command) => {
                        debug!("queueing MULTI command: {command:?}");
                        command_queue.push(command);
                        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
                    }
                    Err(err) => RespValue::Error(Bytes::from(err.to_string())),
                },
            },
            Err(err) => RespValue::Error(Bytes::from(err.to_string())),
        };

        debug!("response: {response:?}");
        if let Err(e) = cxn.send(response).await {
            debug!("exiting transaction due to write error: {e}");
            return None;
        }
    }

    None
}
