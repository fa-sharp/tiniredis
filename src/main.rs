mod parser;
mod request;
mod response;

use anyhow::Context;
use futures::{SinkExt, Stream, TryStreamExt};
use tokio::{
    io::{AsyncWrite, BufWriter},
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    parser::{RedisParseError, RespEncoder, RespParser},
    request::parse_command,
    response::process_command,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("tiniredis listening on port 6379...");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(process(stream));
            }
            Err(e) => eprintln!("Error connecting to client: {e}"),
        }
    }
}

async fn process(mut socket: TcpStream) {
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespParser);
    let mut response_writer = FramedWrite::new(BufWriter::new(writer), RespEncoder);

    if let Err(e) = inner_process(&mut framed_reader, &mut response_writer).await {
        eprintln!("Error processing request: {e}");
    };

    response_writer.flush().await.ok();
}

async fn inner_process<R, W>(
    mut reader: R,
    writer: &mut FramedWrite<W, RespEncoder>,
) -> anyhow::Result<()>
where
    R: Stream<Item = Result<parser::RedisValue, RedisParseError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    while let Some(value) = reader.try_next().await.context("parse request")? {
        println!("Received value: {:?}", value);
        let command = parse_command(value)?;

        let response = process_command(command);
        println!("Sending response: {:?}", response);

        writer.send(response).await.context("write response")?;
    }

    Ok(())
}
