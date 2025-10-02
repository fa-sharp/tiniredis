use std::{collections::VecDeque, sync::Mutex};

use crate::tasks::{BPopClient, PubSubClient, XReadClient};

/// Holds the queues for blocking operations, pub/sub, etc.
#[derive(Debug, Default)]
pub struct Queues {
    bpop: Mutex<VecDeque<BPopClient>>,
    xread: Mutex<VecDeque<XReadClient>>,
    pubsub: Mutex<Vec<PubSubClient>>,
}

impl Queues {
    /// Get an exclusive lock on the blocking pop queue
    pub fn bpop_lock(&self) -> std::sync::MutexGuard<'_, VecDeque<BPopClient>> {
        self.bpop.lock().unwrap()
    }

    /// Enqueue a blocking pop client
    pub fn bpop_push(&self, client: BPopClient) {
        self.bpop_lock().push_back(client);
    }

    /// Get an exclusive lock on the blocking xread queue
    pub fn xread_lock(&self) -> std::sync::MutexGuard<'_, VecDeque<XReadClient>> {
        self.xread.lock().unwrap()
    }

    /// Enqueue a blocking xread client
    pub fn xread_push(&self, client: XReadClient) {
        self.xread_lock().push_back(client);
    }

    /// Get an exclusive lock on the pubsub queue
    pub fn pubsub_lock(&self) -> std::sync::MutexGuard<'_, Vec<PubSubClient>> {
        self.pubsub.lock().unwrap()
    }

    /// Add a pubsub client
    pub fn pubsub_add(&self, client: PubSubClient) {
        self.pubsub_lock().push(client);
    }

    /// Remove any disconnected clients from queues
    pub fn cleanup_disconnected(&self) {
        self.bpop_lock().retain(|client| !client.tx.is_closed());
        self.xread_lock()
            .retain(|client| !client.tx.as_ref().is_none_or(|tx| tx.is_closed()));
        self.pubsub_lock().retain(|client| !client.tx.is_closed());
    }
}
