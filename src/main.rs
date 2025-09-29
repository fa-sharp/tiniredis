mod arguments;
mod command;
mod parser;
mod storage;

use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
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

    println!("starting cleanup task...");
    tokio::spawn(cleanup_task(Arc::clone(&storage), shutdown.clone()));

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("tiniredis listening on port 6379...");

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, _)) => {
                        tokio::spawn(process(stream, Arc::clone(&storage)));
                    }
                    Err(e) => eprintln!("Error connecting to client: {e}"),
                }
            }
            _ = shutdown.changed() => {
                println!("shutdown signal received. goodbye for now 👋");
                break;
            },
        }
    }

    Ok(())
}

async fn process(mut socket: TcpStream, storage: Arc<Mutex<impl storage::Storage>>) {
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespParser);
    let mut response_writer = FramedWrite::new(BufWriter::new(writer), RespEncoder);

    if let Err(e) = inner_process(&mut framed_reader, &mut response_writer, storage).await {
        let error_message = e.to_string();
        eprintln!("Error processing request: {error_message}");
        let root_cause = e.root_cause().to_string();
        if error_message != root_cause {
            eprintln!("Cause: {root_cause}");
        }
    };

    response_writer.flush().await.ok();
}

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
        let response = {
            let mut storage_lock = storage.lock().unwrap();
            command.execute(storage_lock.deref_mut())
        };

        println!("Sending response: {:?}", response);
        writer.send(response).await.context("write response")?;
    }

    Ok(())
}

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
