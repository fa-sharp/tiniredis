use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::debug;

use crate::storage::Storage;

/// Task to periodically cleanup expired keys
pub async fn cleanup_task(
    storage: Arc<Mutex<impl Storage>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = interval.tick() => (),
            _ = shutdown.changed() => break
        }
        debug!("cleaning up expired objects");
        storage.lock().unwrap().cleanup_expired();
    }
}
