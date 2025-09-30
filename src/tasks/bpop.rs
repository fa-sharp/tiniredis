use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;

use crate::{
    queues::Queues,
    storage::{ListDirection, Storage},
};

/// A blocking pop client waiting for a value, stored in storage
#[derive(Debug)]
pub struct BPopClient {
    pub key: Bytes,
    pub dir: ListDirection,
    pub tx: oneshot::Sender<Bytes>,
}

/// Task that manages the queue of blocking pop clients. Listens for changed
/// key events via a channel, and then pops and sends list elements to any
/// waiting clients.
pub async fn bpop_task(
    storage: Arc<Mutex<impl Storage>>,
    queues: Arc<Queues>,
    mut bpop_rx: mpsc::UnboundedReceiver<Bytes>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let key = tokio::select! {
            opt = bpop_rx.recv()=> {
                match opt {
                    Some(key) => key,
                    None => {
                        warn!("bpop task sender was dropped");
                        break;
                    },
                }
            },
            _ = shutdown.changed() => break
        };

        // Get locks on the data storage and bpop queue
        let mut storage_lock = storage.lock().unwrap();
        let mut bpop_queue = queues.bpop_lock();

        // Iterate over the bpop queue, looking for blocking clients waiting on this key.
        while let Some(client_idx) = bpop_queue.iter().position(|c| c.key == key) {
            let direction = bpop_queue[client_idx].dir;
            // Pop the element with the client's chosen direction
            if let Some(mut popped) = storage_lock.pop(&key, direction, 1) {
                // Remove the blocking client from the queue
                let client = bpop_queue.remove(client_idx).expect("valid");
                let elem = popped.pop().expect("should have 1 item");

                // Send the response to client
                if let Err(elem) = client.tx.send(elem) {
                    // client was dropped, push elem back onto list
                    storage_lock
                        .push(key.clone(), vec![elem].into(), direction)
                        .expect("should be a list");
                }
            } else {
                // No more elements in list
                break;
            }
        }
    }
}
