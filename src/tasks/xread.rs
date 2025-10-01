use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    queues::Queues,
    storage::stream::{StreamEntry, StreamStorage},
};

/// A blocking xread client waiting for an added stream value
#[derive(Debug)]
pub struct XReadClient {
    pub streams: Vec<(Bytes, Bytes)>,
    pub tx: Option<oneshot::Sender<Result<Vec<(Bytes, Vec<StreamEntry>)>, Bytes>>>,
}

/// Task that manages the queue of blocking xread clients. Listens for XADD
/// events via a channel.
pub async fn xread_task(
    storage: Arc<Mutex<impl StreamStorage>>,
    queues: Arc<Queues>,
    mut xadd_rx: mpsc::UnboundedReceiver<Bytes>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let key = tokio::select! {
            opt = xadd_rx.recv()=> {
                match opt {
                    Some(key) => key,
                    None => break,
                }
            },
            _ = shutdown.changed() => break
        };

        // Get locks on the data storage and xread queue
        let storage_lock = storage.lock().unwrap();
        let mut xread_queue = queues.xread_lock();

        // Iterate over the xread queue, looking for blocking clients waiting on this stream.
        for client in xread_queue
            .iter_mut()
            .filter(|client| client.streams.iter().any(|(k, _id)| *k == key))
        {
            // Check if this client's channel has been dropped, remove sender if so
            if client.tx.as_ref().is_some_and(|tx| tx.is_closed()) {
                client.tx.take();
                continue;
            }

            // Execute XREAD command, and if not an empty response, remove sender and send response to client
            match storage_lock.xread(client.streams.clone()) {
                Ok((_, response)) if !response.is_empty() => {
                    client.tx.take().and_then(|tx| tx.send(Ok(response)).ok());
                }
                Err(err) => {
                    client.tx.take().and_then(|tx| tx.send(Err(err)).ok());
                }
                _ => {}
            };
        }

        // Remove handled clients from queue
        xread_queue.retain(|client| client.tx.is_some());
    }
}
