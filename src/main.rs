use bytes::Bytes;
use clap::Parser;

mod arguments;
mod command;
mod notifiers;
mod pubsub;
mod queues;
mod rdb;
mod server;
mod storage;
mod tasks;
mod transaction;

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Require a password to authenticate before sending commands
    #[arg(long("requirepass"))]
    pass: Option<Bytes>,
    /// The path to the directory where the RDB file is stored
    #[arg(long("dir"))]
    dir_path: Option<String>,
    /// The name of the RDB file
    #[arg(long("dbfilename"))]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = server::Config {
        auth: args.pass,
        rdb_dir: args.dir_path,
        rdb_filename: args.dbfilename,
    };

    server::start_server(config).await
}
