mod arguments;
mod command;
mod parser;
mod storage;

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
    io::{AsyncWrite, BufWriter},
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::{command::Command, parser::*};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
        .init();
    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format::json())
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut shutdown = setup_shutdown_signal();
    let storage: Arc<Mutex<storage::MemoryStorage>> = Arc::default();

    tokio::spawn(cleanup_task(Arc::clone(&storage), shutdown.clone()));

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    info!("tinikeyval listening on {host}:{port}...");

    tokio::select! {
        _ = main_loop(listener, storage) => {}
        _ = shutdown.changed() => {
            info!("shutdown signal received. goodbye for now 👋");
        },
    }

    Ok(())
}

async fn main_loop(
    listener: TcpListener,
    storage: Arc<Mutex<impl storage::Storage + Send + 'static>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(process(stream, Arc::clone(&storage)));
            }
            Err(e) => warn!("Error connecting to client: {e}"),
        }
    }
}

/// Create framed reader and writer to decode and encode RESP frames, and forward to
/// inner process to handle the request.
async fn process(mut socket: TcpStream, storage: Arc<Mutex<impl storage::Storage>>) {
    debug!("New connection");
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespParser);
    let mut response_writer = FramedWrite::new(BufWriter::new(writer), RespEncoder);

    // Forward reader values to inner process to handle the incoming command(s).
    // Catch any bubbled errors and try sending them to client.
    while let Some(value) = framed_reader.next().await {
        if let Err(e) = inner_process(value, &mut response_writer, &storage).await {
            let message = e.to_string();
            warn!("Error processing request: {message}");
            let error_value = RedisValue::Error(Bytes::from(message.into_bytes()));
            response_writer.send(error_value).await.ok();
        };
    }

    response_writer.flush().await.ok();
}

/// Parse, execute, and respond to the incoming command
async fn inner_process<W>(
    read_result: Result<RedisValue, RedisParseError>,
    writer: &mut FramedWrite<W, RespEncoder>,
    storage: &Mutex<impl storage::Storage>,
) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let value = read_result.context("parse request")?;
    debug!("Received value: {:?}", value);

    let command = Command::from_value(value)?;
    debug!("Parsed command: {:?}", command);

    let response = {
        let mut storage_lock = storage.lock().unwrap();
        command.execute(storage_lock.deref_mut())
    };
    debug!("Sending response: {:?}", response);
    writer.send(response).await.context("write response")?;

    Ok(())
}

/// Cleanup expired keys
async fn cleanup_task(
    storage: Arc<Mutex<impl storage::Storage>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = interval.tick() => (),
            _ = shutdown.changed() => break
        }
        storage.lock().unwrap().cleanup();
    }
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
