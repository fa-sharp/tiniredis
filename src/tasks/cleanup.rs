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
        debug!("cleanup task running");
        storage.lock().unwrap().cleanup_expired();
        queues.bpop_lock().retain(|client| !client.tx.is_closed());
    }
}
