use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::debug;

use crate::{parser::RedisValue, queues::Queues};

/// A pubsub client subscribed to one or more channels
#[derive(Debug)]
pub struct PubSubClient {
    pub channels: HashSet<Bytes>,
    pub tx: mpsc::UnboundedSender<RedisValue>,
}

/// Task that listens for pubsub messages and sends to subscribed clients
#[tracing::instrument(skip(queues, pubsub_rx, shutdown))]
pub async fn pubsub_task(
    queues: Arc<Queues>,
    mut pubsub_rx: mpsc::UnboundedReceiver<(Bytes, Bytes, oneshot::Sender<i64>)>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let (channel, message, send_count_tx) = tokio::select! {
            opt = pubsub_rx.recv()=> {
                match opt {
                    Some(val) => val,
                    None => break,
                }
            },
            _ = shutdown.changed() => break
        };

        // Get lock on the pubsub queue
        let pubsub_queue = queues.pubsub_lock();

        // Iterate over the pubsub queue, looking for clients subscribed to the channel
        let mut send_count = 0;
        for client in pubsub_queue
            .iter()
            .filter(|client| client.channels.contains(&channel))
        {
            // Send message to subscribed clients
            let message_val = RedisValue::Array(vec![
                RedisValue::String(Bytes::from_static(b"message")),
                RedisValue::String(channel.clone()),
                RedisValue::String(message.clone()),
            ]);
            debug!("Sending message to client for channel {channel:?}: {message_val:?}");
            if client.tx.send(message_val).is_ok() {
                send_count += 1;
            }
        }

        // Send back the number of clients that message was sent to
        send_count_tx.send(send_count).ok();
    }
}
