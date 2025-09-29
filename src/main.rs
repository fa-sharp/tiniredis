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
use futures::{SinkExt, Stream, TryStreamExt};
use tokio::{
    io::{AsyncWrite, BufWriter},
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{command::Command, parser::*};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut shutdown = setup_shutdown_signal();
    let storage: Arc<Mutex<storage::MemoryStorage>> = Arc::default();

    tokio::spawn(cleanup_task(Arc::clone(&storage), shutdown.clone()));

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    println!("tiniredis listening on {host}:{port}...");

    tokio::select! {
        _ = main_loop(listener, storage) => {}
        _ = shutdown.changed() => {
            println!("shutdown signal received. goodbye for now 👋");
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
            Err(e) => eprintln!("Error connecting to client: {e}"),
        }
    }
}

/// Create framed reader and writer to decode and encode RESP frames, and forward to
/// inner process to handle the request.
async fn process(mut socket: TcpStream, storage: Arc<Mutex<impl storage::Storage>>) {
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespParser);
    let mut response_writer = FramedWrite::new(BufWriter::new(writer), RespEncoder);

    // Forward to inner process to handle the incoming command(s).
    // Catch any bubbled errors and try sending them to client.
    if let Err(e) = inner_process(&mut framed_reader, &mut response_writer, storage).await {
        let message = e.to_string();
        eprintln!("Error processing request: {message}");
        let error_value = RedisValue::Error(Bytes::from(message.into_bytes()));
        response_writer.send(error_value).await.ok();
    };

    response_writer.flush().await.ok();
}

/// Parse, execute, and respond to the incoming command(s)
async fn inner_process<R, W>(
    mut reader: R,
    writer: &mut FramedWrite<W, RespEncoder>,
    storage: Arc<Mutex<impl storage::Storage>>,
) -> anyhow::Result<()>
where
    R: Stream<Item = Result<parser::RedisValue, RedisParseError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    while let Some(value) = reader.try_next().await.context("parse request")? {
        println!("Received value: {:?}", value);

        let command = Command::from_value(value)?;
        println!("Parsed command: {:?}", command);
        let response = {
            let mut storage_lock = storage.lock().unwrap();
            command.execute(storage_lock.deref_mut())
        };

        println!("Sending response: {:?}", response);
        writer.send(response).await.context("write response")?;
    }

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
        println!("\nsending shutdown signal...");
        shutdown_tx
            .send(true)
            .expect("Failed to send shutdown signal");
    })
    .expect("Failed to setup shutdown handler");

    shutdown_rx
}
