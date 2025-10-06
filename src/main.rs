use std::path::Path;

use bytes::Bytes;
use clap::Parser;

mod arguments;
mod command;
mod pubsub;
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
    /// Save a DB snapshot after the given number of seconds and write operations
    #[arg(long, num_args(2), value_names(["seconds", "changes"]), default_values(["60", "300"]))]
    save: Vec<u32>,
    /// The path to the directory where the RDB file is stored
    #[arg(long, name("path"), default_value("."))]
    dir: String,
    /// The name of the RDB file
    #[arg(long, name("filename"), default_value("dump.rdb"))]
    dbfilename: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = server::Config {
        auth: args.requirepass,
        rdb_path: Path::new(&args.dir).join(&args.dbfilename),
        rdb_dir: args.dir,
        rdb_filename: args.dbfilename,
        persist: (args.save[0].into(), args.save[1].try_into().unwrap_or(300)),
    };

    server::start_server(config).await
}
