use std::{collections::VecDeque, sync::Mutex};

use bytes::Bytes;
use fxhash::FxHashMap;
use tinikeyval_protocol::RespValue;
use tokio::sync::{mpsc, oneshot};

use crate::storage::{list::ListDirection, stream::StreamKeyAndEntries};

use super::{bpop::BPopClient, pubsub::PubSubClient, xread::XReadClient};

/// Holds the queues for blocking operations, pub/sub, etc.
#[derive(Debug, Default)]
pub struct Queues {
    bpop: Mutex<VecDeque<BPopClient>>,
    xread: Mutex<Vec<XReadClient>>,
    pubsub: Mutex<FxHashMap<u64, PubSubClient>>,
}

impl Queues {
    /// Enqueue a blocking pop client
    pub fn bpop_push(&self, key: Bytes, dir: ListDirection, tx: oneshot::Sender<Bytes>) {
        self.bpop_lock().push_back(BPopClient { key, dir, tx });
    }

    /// Add a blocking xread client
    pub fn xread_push(
        &self,
        streams: Vec<(Bytes, Bytes)>,
        tx: oneshot::Sender<Result<Vec<StreamKeyAndEntries>, Bytes>>,
    ) {
        self.xread_lock().push(XReadClient {
            streams,
            tx: Some(tx),
        });
    }
    /// Add a pubsub client and get its ID
    pub fn pubsub_add(&self, tx: mpsc::UnboundedSender<RespValue>) -> u64 {
        let mut pubsub_lock = self.pubsub_lock();
        let mut id: u64 = rand::random();
        while pubsub_lock.contains_key(&id) {
            id = rand::random()
        }
        pubsub_lock.insert(id, PubSubClient::new(tx));

        id
    }

    /// Get an exclusive lock on the blocking pop queue
    pub(super) fn bpop_lock(&self) -> std::sync::MutexGuard<'_, VecDeque<BPopClient>> {
        self.bpop.lock().unwrap()
    }

    /// Get an exclusive lock on the blocking xread clients
    pub(super) fn xread_lock(&self) -> std::sync::MutexGuard<'_, Vec<XReadClient>> {
        self.xread.lock().unwrap()
    }

    /// Get an exclusive lock on the pubsub clients
    pub(super) fn pubsub_lock(&self) -> std::sync::MutexGuard<'_, FxHashMap<u64, PubSubClient>> {
        self.pubsub.lock().unwrap()
    }

    /// Remove any disconnected/defunct clients
    pub(super) fn cleanup_disconnected(&self) {
        self.bpop_lock().retain(|client| !client.tx.is_closed());
        self.xread_lock()
            .retain(|client| !client.tx.as_ref().is_none_or(|tx| tx.is_closed()));
        self.pubsub_lock()
            .retain(|_, client| !client.tx.is_closed());
    }
}
