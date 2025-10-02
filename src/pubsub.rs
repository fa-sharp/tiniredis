use anyhow::{bail, Context};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::debug;

use crate::{
    arguments::Arguments,
    notifiers::Notifiers,
    parser::{RedisValue, RespDecoder, RespEncoder},
};

/// 'Subscribe mode' for pubsub clients. Only responds to a subset of commands.
#[tracing::instrument(skip(rx, notifiers, reader, writer))]
pub async fn subscribe_mode(
    client_id: u64,
    mut rx: mpsc::UnboundedReceiver<RedisValue>,
    notifiers: &Notifiers,
    reader: &mut FramedRead<impl AsyncBufRead + Unpin, RespDecoder>,
    writer: &mut FramedWrite<impl AsyncWrite + Unpin, RespEncoder>,
) {
    loop {
        let response = tokio::select! {
            Some(message) = rx.recv() => {
                debug!("message received: {message:?}");
                message
            }
            Some(reader_res) = reader.next() => {
                match reader_res {
                    Ok(raw_command) => match process_pubsub_command(raw_command, client_id, notifiers) {
                        Ok(_) => continue,
                        Err(err) => RedisValue::Error(Bytes::from(err.to_string())),
                    },
                    Err(err) => {
                        debug!("exiting subscribe mode due to read error: {err}");
                        break;
                    }
                }
            }
            else => break
        };

        debug!("response: {response:?}");
        if let Err(e) = writer.send(response).await {
            debug!("exiting subscribe mode due to write error: {e}");
            break;
        }
    }
}

/// Process a command in 'subscribe mode'
fn process_pubsub_command(
    raw_command: RedisValue,
    client_id: u64,
    notifiers: &Notifiers,
) -> anyhow::Result<()> {
    debug!("Received command: {raw_command:?}");
    let mut args = Arguments::from_raw_value(raw_command)?;

    match args.command() {
        "PING" => notifiers.pubsub_ping(client_id).context("pubsub receiver dropped")?,
        "SUBSCRIBE" => {
            let mut channels = vec![args.pop("channel")?];
            while let Some(channel) = args.pop_optional() {
                channels.push(channel);
            }
            notifiers.pubsub_subscribe(client_id, channels).context("pubsub receiver dropped")?;
        },
        "UNSUBSCRIBE" => {
            let mut channels = Vec::new();
            while let Some(channel) = args.pop_optional() {
                channels.push(channel);
            }
            notifiers.pubsub_unsubscribe(client_id, channels).context("pubsub receiver dropped")?;
        },
        cmd => bail!("ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"),
    };

    Ok(())
}
