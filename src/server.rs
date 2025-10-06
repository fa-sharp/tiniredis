mod process;
mod shutdown;

use std::{
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use tokio::{net::TcpListener, task::spawn_blocking};
use tracing::{debug, info, warn};

use crate::{
    storage::{rdb, MemoryStorage, Storage},
    tasks::{spawn_server_tasks, Notifiers, Queues},
};

/// Server config
#[derive(Debug, Default)]
pub struct Config {
    pub auth: Option<Bytes>,
    pub rdb_dir: String,
    pub rdb_filename: String,
    pub rdb_path: PathBuf,
    pub persist: (u64, usize),
}

/// Setup the server and start listening for connections
pub async fn start_server(config: Config) -> anyhow::Result<()> {
    // Setup logging
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
        .init();

    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format::json().flatten_event(true))
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Configuration
    let config = Arc::new(config);

    // Setup and load storage from RDB file
    let rdb_file_path = config.rdb_path.to_owned();
    let storage = match spawn_blocking(move || rdb::load_rdb_file(&rdb_file_path)).await {
        Ok(Ok(storage)) => {
            info!("Database loaded from file, keys loaded: {}", storage.size());
            Arc::new(Mutex::new(storage))
        }
        Ok(Err(err)) => {
            warn!("Failed loading database file: {err} ({})", err.root_cause());
            Arc::default()
        }
        Err(err) => panic!("Database read task panicked: {err}"),
    };

    // Spawn all tasks
    let mut shutdown_sig = shutdown::setup_shutdown_signal();
    let (all_tasks, queues, notifiers) = spawn_server_tasks(&storage, &config, &shutdown_sig);

    // Start server
    let host_var = env::var("HOST");
    let port_var = env::var("PORT");
    let host = host_var.as_deref().unwrap_or("127.0.0.1");
    let port = port_var.as_deref().unwrap_or("6379");
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    info!("tinikeyval listening on {host}:{port}...");

    tokio::select! {
        _ = main_loop(listener, config, storage, queues, notifiers) => {}
        _ = shutdown_sig.changed() => {
            info!("shutdown signal received. shutting down...");
        },
    }

    // Ensure all tasks have shut down
    tokio::time::timeout(Duration::from_secs(5), all_tasks.join_all())
        .await
        .context("some task(s) didn't shut down within grace period")?;
    info!("server shut down. goodbye for now 👋");
    Ok(())
}

async fn main_loop(
    listener: TcpListener,
    config: Arc<Config>,
    storage: Arc<Mutex<MemoryStorage>>,
    queues: Arc<Queues>,
    notifiers: Arc<Notifiers>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("New connection from {addr}");
                tokio::spawn(process::process_incoming(
                    stream,
                    Arc::clone(&config),
                    Arc::clone(&storage),
                    Arc::clone(&queues),
                    Arc::clone(&notifiers),
                ));
            }
            Err(e) => warn!("Error connecting to client: {e}"),
        }
    }
}
