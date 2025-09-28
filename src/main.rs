use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
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

async fn process(socket: TcpStream) -> anyhow::Result<()> {
    let mut connection = BufWriter::new(socket);

    let c = connection.read_u8().await?;
    println!("Received '{c}'");

    if let Err(e) = connection.write_all(b"+PONG\r\n").await {
        eprintln!("Error sending response: {e}")
    }
    connection.flush().await?;

    Ok(())
}
