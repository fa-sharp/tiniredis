use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tinikeyval_protocol::{constants, RedisParseError, RespCodec, RespValue};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
};
use tracing::{debug, info, warn};

use crate::{
    command::{Command, CommandResponse},
    notifiers::Notifiers,
    pubsub,
    queues::Queues,
    storage::MemoryStorage,
    transaction::process_transaction,
};

/// Process incoming connection - wrap the connection with a RESP framer, and then
/// process and respond to incoming commands.
pub async fn process_incoming(
    mut tcp_stream: TcpStream,
    config: Arc<super::Config>,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    notifiers: Arc<Notifiers>,
) {
    let mut cxn = RespCodec::framed_io(BufWriter::new(BufReader::new(&mut tcp_stream)));
    let mut authed = !config.auth.is_some();

    while let Some(value) = cxn.next().await {
        let response =
            match process_command(value, authed, &config, &storage, &queues, &notifiers).await {
                Ok(command_result) => {
                    let response_result = match command_result {
                        Ok(CommandResponse::Auth(pass)) => {
                            if config.auth.as_deref().is_none_or(|a| a == pass.as_ref()) {
                                authed = true;
                                Ok(constants::OK)
                            } else {
                                authed = false;
                                Err(Bytes::from("WRONGPASS invalid password"))
                            }
                        }
                        Ok(CommandResponse::Value(value)) => Ok(value),
                        Ok(CommandResponse::Block(rx)) => match rx.await {
                            Ok(res) => res,
                            Err(_) => Err(Bytes::from("Failed to receive message")),
                        },
                        Ok(CommandResponse::Subscribed(id, rx)) => {
                            debug!("Entering subscribe mode");
                            pubsub::subscribe_mode(id, rx, &notifiers, &mut cxn).await;
                            continue;
                        }
                        Ok(CommandResponse::Transaction) => {
                            debug!("Starting MULTI transaction");
                            cxn.send(tinikeyval_protocol::constants::OK).await.ok();
                            let Some(command_queue) = process_transaction(&mut cxn).await else {
                                debug!("Exiting MULTI transaction - no commands received");
                                continue;
                            };

                            debug!("Executing MULTI commands: {command_queue:?}");
                            let responses = {
                                let mut storage_lock = storage.lock().unwrap();
                                let mut responses = Vec::with_capacity(command_queue.len());
                                for command in command_queue {
                                    match command.execute(
                                        storage_lock.deref_mut(),
                                        &config,
                                        &queues,
                                        &notifiers,
                                    ) {
                                        Ok(response) => match response {
                                            CommandResponse::Value(value) => responses.push(value),
                                            _ => responses.push(RespValue::Error(Bytes::from(
                                                "ERR Unsupported operation in MULTI block",
                                            ))),
                                        },
                                        Err(err) => responses.push(RespValue::Error(err)),
                                    }
                                }
                                responses
                            };
                            Ok(RespValue::Array(responses))
                        }
                        Err(err) => Err(err),
                    };
                    match response_result {
                        Ok(val) => val,
                        Err(err) => RespValue::Error(err),
                    }
                }
                Err(err) => {
                    let message = err.to_string();
                    info!("Error processing command: {message}");
                    RespValue::Error(Bytes::from(message.into_bytes()))
                }
            };

        debug!("Response: {:?}", response);
        let write_err = if cxn.read_buffer().len() > 0 {
            cxn.feed(response).await.err() // feed response if reader has more data (e.g. client is pipelining)
        } else {
            cxn.send(response).await.err()
        };
        if let Some(err) = write_err {
            warn!("Failed to send response: {err}");
            break;
        }
    }

    drop(cxn);
    tcp_stream.shutdown().await.ok();
}

/// Parse, execute, and respond to the incoming command
async fn process_command(
    value: Result<RespValue, RedisParseError>,
    authed: bool,
    config: &super::Config,
    storage: &Mutex<MemoryStorage>,
    queues: &Queues,
    notifiers: &Notifiers,
) -> anyhow::Result<Result<CommandResponse, Bytes>> {
    let value = value?;
    debug!("Received value: {:?}", value);

    let command = Command::from_value(value)?;
    debug!("Parsed command: {:?}", command);

    if !authed {
        match &command {
            Command::Auth(_) => {}
            _ => anyhow::bail!("NOAUTH Authentication required"),
        }
    }

    let command_response = {
        let mut storage_lock = storage.lock().unwrap();
        command.execute(storage_lock.deref_mut(), config, queues, notifiers)
    };
    Ok(command_response)
}
