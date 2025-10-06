use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    task::spawn_blocking,
    time::{interval, Instant},
};
use tracing::{info, instrument, warn};

use crate::{
    server::Config,
    storage::{rdb::save_rdb_file, MemoryStorage},
    tasks::counters::ChangeCounter,
};

/// Task that periodically saves a snapshot of the database to an RDB file
#[instrument(skip_all)]
pub async fn persist_task(
    storage: Arc<Mutex<MemoryStorage>>,
    counter: Arc<ChangeCounter>,
    config: Arc<Config>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut last_save = Instant::now();
    let mut interval = interval(Duration::from_secs(5));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {},
            _ = shutdown.changed() => break
        };

        let (secs, changes) = config.persist;
        if counter.load() >= changes && Instant::now().duration_since(last_save).as_secs() > secs {
            info!("{changes} changes and >{secs} seconds since last save. Saving database snapshot...");
            counter.reset();
            last_save = Instant::now();
        } else {
            continue;
        }

        let storage = Arc::clone(&storage);
        let file_path = config.rdb_path.to_owned();
        match spawn_blocking(move || save_rdb_file(&storage, &file_path)).await {
            Ok(Err(err)) => warn!("Error saving database: {err} ({})", err.root_cause()),
            Err(err) => warn!("Panic while saving database: {err}"),
            _ => {}
        }
    }

    info!("Saving final snapshot before shutting down...");
    let storage = Arc::clone(&storage);
    let file_path = config.rdb_path.to_owned();
    match spawn_blocking(move || save_rdb_file(&storage, &file_path)).await {
        Ok(Err(err)) => warn!("Error saving database: {err} ({})", err.root_cause()),
        Err(err) => warn!("Panic while saving database: {err}"),
        _ => {}
    }
}
