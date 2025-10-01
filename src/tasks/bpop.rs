use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    queues::Queues,
    storage::list::{ListDirection, ListStorage},
};

/// A blocking pop client waiting for a value
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
    storage: Arc<Mutex<impl ListStorage>>,
    queues: Arc<Queues>,
    mut bpop_rx: mpsc::UnboundedReceiver<Bytes>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let key = tokio::select! {
            opt = bpop_rx.recv()=> {
                match opt {
                    Some(key) => key,
                    None => break,
                }
            },
            _ = shutdown.changed() => break
        };

        // Get locks on the data storage and bpop queue
        let mut storage_lock = storage.lock().unwrap();
        let mut bpop_queue = queues.bpop_lock();

        // Iterate over the bpop queue, looking for blocking clients waiting on this key.
        while let Some(client_idx) = bpop_queue.iter().position(|c| c.key == key) {
            // Check if this client's channel/receiver has been dropped
            if bpop_queue[client_idx].tx.is_closed() {
                bpop_queue.remove(client_idx);
                continue;
            }

            // Pop the element with the client's chosen direction
            let direction = bpop_queue[client_idx].dir;
            if let Some(mut popped) = storage_lock.pop(&key, direction, 1) {
                // Remove the blocking client from the queue
                let client = bpop_queue.remove(client_idx).expect("valid idx");
                let elem = popped.pop().expect("pop() should return 1 item");

                // Send the response to client
                client.tx.send(elem).ok();
            } else {
                // No more elements in list
                break;
            }
        }
    }
}
