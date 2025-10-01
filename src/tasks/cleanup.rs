use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::debug;

use crate::{queues::Queues, storage::Storage};

/// Task to periodically cleanup expired keys and disconnected blocking clients
pub async fn cleanup_task(
    storage: Arc<Mutex<impl Storage>>,
    queues: Arc<Queues>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = interval.tick() => (),
            _ = shutdown.changed() => break
        }

        let expired_count = storage.lock().unwrap().cleanup_expired();
        queues.cleanup_disconnected();

        debug!("cleanup task: {expired_count} expired");
    }
}
