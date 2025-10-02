use std::{collections::VecDeque, sync::Mutex};

use fxhash::FxHashMap;
use tokio::sync::mpsc;

use crate::{
    parser::RedisValue,
    tasks::{BPopClient, PubSubClient, XReadClient},
};

/// Holds the queues for blocking operations, pub/sub, etc.
#[derive(Debug, Default)]
pub struct Queues {
    bpop: Mutex<VecDeque<BPopClient>>,
    xread: Mutex<Vec<XReadClient>>,
    pubsub: Mutex<FxHashMap<u64, PubSubClient>>,
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

    /// Get an exclusive lock on the blocking xread clients
    pub fn xread_lock(&self) -> std::sync::MutexGuard<'_, Vec<XReadClient>> {
        self.xread.lock().unwrap()
    }

    /// Add a blocking xread client
    pub fn xread_push(&self, client: XReadClient) {
        self.xread_lock().push(client);
    }

    /// Get an exclusive lock on the pubsub clients
    pub fn pubsub_lock(&self) -> std::sync::MutexGuard<'_, FxHashMap<u64, PubSubClient>> {
        self.pubsub.lock().unwrap()
    }

    /// Add a pubsub client and get its ID
    pub fn pubsub_add(&self, tx: mpsc::UnboundedSender<RedisValue>) -> u64 {
        let mut pubsub_lock = self.pubsub_lock();
        let mut id: u64 = rand::random();
        while pubsub_lock.contains_key(&id) {
            id = rand::random()
        }
        pubsub_lock.insert(id, PubSubClient::new(tx));

        id
    }

    /// Remove any disconnected/defunct clients
    pub fn cleanup_disconnected(&self) {
        self.bpop_lock().retain(|client| !client.tx.is_closed());
        self.xread_lock()
            .retain(|client| !client.tx.as_ref().is_none_or(|tx| tx.is_closed()));
        self.pubsub_lock()
            .retain(|_, client| !client.tx.is_closed());
    }
}
