mod parser;
mod request;
mod response;

use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::{parser::RespParser, request::parse_command, response::process_command};

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

async fn process(mut socket: TcpStream) -> anyhow::Result<()> {
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, RespParser);
    let mut response_writer = BufWriter::new(writer);

    while let Some(res) = framed_reader.next().await {
        let value = match res {
            Ok(value) => value,
            Err(e) => {
                eprintln!("Error parsing value: {e}");
                continue;
            }
        };
        println!("Received value {:?}", value);
        let command = match parse_command(value) {
            request::ParsedCommand::Command(command) => command,
            request::ParsedCommand::NotEnoughBytes => todo!(),
        };

        let response = process_command(&command);
        println!(
            "Sending response:\n{}\n",
            String::from_utf8_lossy(response).escape_debug()
        );

        if let Err(e) = response_writer.write_all(response).await {
            eprintln!("Error sending last response: {e}\n")
        }
        response_writer.flush().await.ok();
    }

    Ok(())
}
