use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{task::spawn_blocking, time::interval};
use tracing::{info, instrument, warn};

use crate::storage::{rdb::save_rdb_file, MemoryStorage};

/// Task that periodically saves a snapshot of the database to an RDB file
#[instrument(skip_all)]
pub async fn persist_task(
    storage: Arc<Mutex<MemoryStorage>>,
    file_path: PathBuf,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = interval(Duration::from_secs(60));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => (),
            _ = shutdown.changed() => break
        }

        info!("Saving database snapshot...");
        let storage = Arc::clone(&storage);
        let file_path_owned = file_path.to_owned();
        match spawn_blocking(move || save_rdb_file(&storage, &file_path_owned)).await {
            Ok(Err(err)) => warn!("Error saving database: {err} ({})", err.root_cause()),
            Err(err) => warn!("Panic while saving database: {err}"),
            _ => {}
        }
    }

    info!("Final save of database snapshot before shutting down...");
    let storage = Arc::clone(&storage);
    let file_path = file_path.to_owned();
    match spawn_blocking(move || save_rdb_file(&storage, &file_path)).await {
        Ok(Err(err)) => warn!("Error saving database: {err} ({})", err.root_cause()),
        Err(err) => warn!("Panic while saving database: {err}"),
        _ => {}
    }
}
