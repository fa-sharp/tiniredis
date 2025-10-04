mod process;
mod shutdown;

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};
use tracing::{debug, info, warn};

use crate::{notifiers::Notifiers, queues::Queues, storage::MemoryStorage, tasks};

/// Server config
#[derive(Debug, Default)]
pub struct Config {
    pub auth: Option<Bytes>,
    pub rdb_dir: Option<String>,
    pub rdb_filename: Option<String>,
}

pub async fn start_server(config: Config) -> anyhow::Result<()> {
    // Setup logging
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
        .init();

    #[cfg(not(debug_assertions))]
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format::json())
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Configuration
    let config = Arc::new(config);

    // Setup channels
    let (bpop_tx, bpop_rx) = mpsc::unbounded_channel();
    let (xread_tx, xread_rx) = mpsc::unbounded_channel();
    let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

    // Setup storage, queues, and notifiers
    let storage: Arc<Mutex<MemoryStorage>> = Arc::default();
    let queues: Arc<Queues> = Arc::default();
    let notifiers: Arc<Notifiers> = Arc::new(Notifiers {
        bpop: bpop_tx,
        xread: xread_tx,
        pubsub: pubsub_tx,
    });

    // Spawn all tasks
    let mut all_tasks = JoinSet::new();
    let mut shutdown_sig = shutdown::setup_shutdown_signal();
    all_tasks.spawn(tasks::bpop_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        bpop_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::xread_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        xread_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::pubsub_task(
        Arc::clone(&queues),
        pubsub_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(tasks::cleanup_task(
        Arc::clone(&storage),
        Arc::clone(&queues),
        shutdown_sig.clone(),
    ));

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
            info!("shutdown signal received. goodbye for now 👋");
        },
    }

    // Ensure all tasks have shut down
    tokio::time::timeout(Duration::from_secs(5), all_tasks.join_all())
        .await
        .context("some task(s) didn't shut down within grace period")?;
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
