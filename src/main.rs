mod arguments;
mod command;
mod parser;
mod queues;
mod senders;
mod storage;
mod tasks;

use std::{
    env,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::{
    command::{Command, CommandResponse},
    parser::*,
    queues::Queues,
    senders::Senders,
    storage::MemoryStorage,
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

    // Setup storage, queues, and notifiers
    let storage: Arc<Mutex<MemoryStorage>> = Arc::default();
    let queues: Arc<Queues> = Arc::default();
    let senders: Arc<Senders> = Arc::new(Senders {
        bpop: bpop_tx,
        xread: xread_tx,
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
        _ = main_loop(listener, storage, queues, senders) => {}
        _ = shutdown_sig.changed() => {
            info!("shutdown signal received. goodbye for now 👋");
        },
    }

    // Ensure all tasks have shut down
    tokio::time::timeout(Duration::from_secs(5), all_tasks.join_all())
        .await
        .context("task(s) didn't shut down within grace period")?;
    Ok(())
}

async fn main_loop(
    listener: TcpListener,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    senders: Arc<Senders>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("New connection from {addr}");
                tokio::spawn(process(
                    stream,
                    Arc::clone(&storage),
                    Arc::clone(&queues),
                    Arc::clone(&senders),
                ));
            }
            Err(e) => warn!("Error connecting to client: {e}"),
        }
    }
}

/// Create framed reader and writer to decode and encode RESP frames, and forward to
/// inner process to handle the request.
async fn process(
    mut socket: TcpStream,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    senders: Arc<Senders>,
) {
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespDecoder);
    let mut framed_writer = FramedWrite::new(writer, RespEncoder);

    // Forward reader values to inner process to handle the incoming command(s).
    // Catch any bubbled errors and try sending them to the client.
    while let Some(val) = framed_reader.next().await {
        match inner_process(val, &storage, &queues, &senders).await {
            Ok(response) => {
                if let Err(err) = framed_writer.send(response).await {
                    warn!("Failed to send response: {err}");
                    break;
                }
            }
            Err(err) => {
                let message = err.to_string();
                info!("Error processing command: {message}");
                let error_value = RedisValue::Error(Bytes::from(message.into_bytes()));
                framed_writer.send(error_value).await.ok();
            }
        };
    }

    framed_writer.close().await.ok();
}

/// Parse, execute, and respond to the incoming command
async fn inner_process(
    value: Result<RedisValue, RedisParseError>,
    storage: &Mutex<MemoryStorage>,
    queues: &Queues,
    senders: &Senders,
) -> anyhow::Result<RedisValue> {
    let value = value.context("parse request")?;
    debug!("Received value: {:?}", value);

    let command = Command::from_value(value)?;
    debug!("Parsed command: {:?}", command);

    let command_response = {
        let mut storage_lock = storage.lock().unwrap();
        command.execute(storage_lock.deref_mut(), queues, senders)
    };
    let response_result = match command_response {
        Ok(CommandResponse::Value(value)) => Ok(value),
        Ok(CommandResponse::Block(rx)) => rx.await.context("sender dropped")?,
        Err(err) => Err(err),
    };
    let response_val = match response_result {
        Ok(val) => val,
        Err(err) => RedisValue::Error(err),
    };
    debug!("Response: {:?}", response_val);
    Ok(response_val)
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
