mod arguments;
mod command;
mod parser;
mod storage;

use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use futures::{SinkExt, Stream, TryStreamExt};
use tokio::{
    io::{AsyncWrite, BufWriter},
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{command::Command, parser::*};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage: Arc<Mutex<storage::MemoryStorage>> = Arc::default();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("tiniredis listening on port 6379...");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(process(stream, Arc::clone(&storage)));
            }
            Err(e) => eprintln!("Error connecting to client: {e}"),
        }
    }
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
