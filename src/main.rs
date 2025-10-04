mod arguments;
mod command;
mod notifiers;
mod pubsub;
mod queues;
mod storage;
mod tasks;
mod transaction;

use std::{
    env,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tinikeyval_protocol::{RedisParseError, RespValue, RespCodec};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinSet,
};
use tracing::{debug, info, warn};

use crate::{
    command::{Command, CommandResponse},
    notifiers::Notifiers,
    queues::Queues,
    storage::MemoryStorage,
    transaction::process_transaction,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup logging
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
        .init();

    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format::json())
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Setup channels
    let (bpop_tx, bpop_rx) = mpsc::unbounded_channel();
    let (xread_tx, xread_rx) = mpsc::unbounded_channel();
    let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

    // Setup storage, queues, and notifiers
    let storage: Arc<Mutex<MemoryStorage>> = Arc::default();
    let queues: Arc<Queues> = Arc::default();
    let notifiers: Arc<Notifiers> = Arc::new(Notifiers {
        bpop: bpop_tx,
        xread: xread_tx,
        pubsub: pubsub_tx,
    });

    // Spawn tasks
    let mut all_tasks = JoinSet::new();
    let mut shutdown_sig = setup_shutdown_signal();
    all_tasks.spawn(tasks::bpop_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        bpop_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::xread_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        xread_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::pubsub_task(
        Arc::clone(&queues),
        pubsub_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::cleanup_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        shutdown_sig.clone(),
    ));

    // Start server
    let host_var = env::var("HOST");
    let port_var = env::var("PORT");
    let host = host_var.as_deref().unwrap_or("127.0.0.1");
    let port = port_var.as_deref().unwrap_or("6379");
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    info!("tinikeyval listening on {host}:{port}...");

    tokio::select! {
        _ = main_loop(listener, storage, queues, notifiers) => {}
        _ = shutdown_sig.changed() => {
            info!("shutdown signal received. goodbye for now 👋");
        },
    }

    // Ensure all tasks have shut down
    tokio::time::timeout(Duration::from_secs(5), all_tasks.join_all())
        .await
        .context("some task(s) didn't shut down within grace period")?;
    Ok(())
}

async fn main_loop(
    listener: TcpListener,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    notifiers: Arc<Notifiers>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("New connection from {addr}");
                tokio::spawn(process(
                    stream,
                    Arc::clone(&storage),
                    Arc::clone(&queues),
                    Arc::clone(&notifiers),
                ));
            }
            Err(e) => warn!("Error connecting to client: {e}"),
        }
    }
}

/// Create framed reader and writer to decode and encode RESP frames, and then
/// process and respond to incoming commands.
async fn process(
    mut tcp_stream: TcpStream,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    notifiers: Arc<Notifiers>,
) {
    let mut cxn = RespCodec::framed_io(BufWriter::new(BufReader::new(&mut tcp_stream)));

    while let Some(value) = cxn.next().await {
        let response = match process_command(value, &storage, &queues, &notifiers).await {
            Ok(command_result) => {
                let response_result = match command_result {
                    Ok(CommandResponse::Value(value)) => Ok(value),
                    Ok(CommandResponse::Block(rx)) => match rx.await {
                        Ok(res) => res,
                        Err(_) => Err(Bytes::from_static(b"Failed to receive message")),
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
                                match command.execute(storage_lock.deref_mut(), &queues, &notifiers)
                                {
                                    Ok(response) => match response {
                                        CommandResponse::Value(value) => responses.push(value),
                                        _ => responses.push(RespValue::Error(Bytes::from_static(
                                            b"ERR Unsupported operation in MULTI block",
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
    storage: &Mutex<MemoryStorage>,
    queues: &Queues,
    notifiers: &Notifiers,
) -> anyhow::Result<Result<CommandResponse, Bytes>> {
    let value = value?;
    debug!("Received value: {:?}", value);

    let command = Command::from_value(value)?;
    debug!("Parsed command: {:?}", command);

    let command_response = {
        let mut storage_lock = storage.lock().unwrap();
        command.execute(storage_lock.deref_mut(), queues, notifiers)
    };
    Ok(command_response)
}

/// For graceful shutdown, setup a signal listener with tokio watch channel
fn setup_shutdown_signal() -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    ctrlc::set_handler(move || {
        info!("sending shutdown signal...");
        shutdown_tx
            .send(true)
            .expect("Failed to send shutdown signal");
    })
    .expect("Failed to setup shutdown handler");

    shutdown_rx
}
