use std::path::Path;

use bytes::Bytes;
use clap::Parser;

mod arguments;
mod command;
mod notifiers;
mod pubsub;
mod queues;
mod server;
mod storage;
mod tasks;
mod transaction;

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// Require a password to authenticate before sending commands
    #[arg(long, name("password"))]
    requirepass: Option<Bytes>,
    /// The path to the directory where the RDB file is stored
    #[arg(long, name("path"))]
    dir: Option<String>,
    /// The name of the RDB file
    #[arg(long, name("filename"))]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = server::Config {
        auth: args.requirepass,
        rdb_path: Path::new(args.dir.as_deref().unwrap_or("."))
            .join(args.dbfilename.as_deref().unwrap_or("dump.rdb")),
        rdb_dir: args.dir,
        rdb_filename: args.dbfilename,
    };

    server::start_server(config).await
}
