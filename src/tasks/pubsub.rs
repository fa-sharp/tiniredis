use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

use crate::{parser::RedisValue, queues::Queues};

/// A pubsub client subscribed to one or more channels
#[derive(Debug)]
pub struct PubSubClient {
    pub tx: mpsc::UnboundedSender<RedisValue>,
    pub channels: HashSet<Bytes>,
}
impl PubSubClient {
    pub fn new(tx: mpsc::UnboundedSender<RedisValue>) -> Self {
        Self {
            tx,
            channels: HashSet::new(),
        }
    }
}

/// An event sent to the pubsub task
pub enum PubSubEvent {
    /// Ping from a pubsub client
    Ping(u64),
    /// A message sent to a channel, with a sender to respond with the
    /// number of clients it was sent to: `(channel, message, sender)`
    Message(Bytes, Bytes, oneshot::Sender<i64>),
    /// Subscribe a client to given channel(s)
    Subscribe(u64, Vec<Bytes>),
    /// Unsubscribe a client from given channel(s)
    Unsubscribe(u64, Vec<Bytes>),
}

/// Task that listens to and handles pubsub events
#[tracing::instrument(skip(queues, pubsub_rx, shutdown))]
pub async fn pubsub_task(
    queues: Arc<Queues>,
    mut pubsub_rx: mpsc::UnboundedReceiver<PubSubEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // Receive an event
        let event = tokio::select! {
            opt = pubsub_rx.recv()=> {
                match opt {
                    Some(val) => val,
                    None => break,
                }
            },
            _ = shutdown.changed() => break
        };

        // Get lock on the pubsub queue
        let mut pubsub_queue = queues.pubsub_lock();

        match event {
            PubSubEvent::Ping(id) => {
                let Some(client) = pubsub_queue.get_mut(&id) else {
                    warn!("pubsub client {id} not found");
                    continue;
                };
                let pong = RedisValue::Array(vec![
                    RedisValue::String(Bytes::from_static(b"pong")),
                    RedisValue::String(Bytes::new()),
                ]);
                client.tx.send(pong).ok();
            }
            PubSubEvent::Subscribe(id, channels) => {
                let Some(client) = pubsub_queue.get_mut(&id) else {
                    warn!("pubsub client {id} not found");
                    continue;
                };

                let mut messages = Vec::new();
                let mut num_subscribed = client.channels.len().try_into().unwrap_or_default();
                for channel in channels {
                    if client.channels.insert(channel.clone()) {
                        num_subscribed += 1;
                    }
                    messages.push(RedisValue::Array(vec![
                        RedisValue::String(Bytes::from_static(b"subscribe")),
                        RedisValue::String(channel.clone()),
                        RedisValue::Int(num_subscribed),
                    ]));
                }

                for message in messages {
                    client.tx.send(message).ok();
                }
            }
            PubSubEvent::Unsubscribe(id, channels) => {
                let Some(client) = pubsub_queue.get_mut(&id) else {
                    warn!("pubsub client {id} not found");
                    continue;
                };

                let mut messages = Vec::new();
                let mut num_subscribed = client.channels.len().try_into().unwrap_or_default();
                if channels.is_empty() {
                    for channel in client.channels.drain() {
                        num_subscribed -= 1;
                        messages.push(unsubscribe_message(channel, num_subscribed));
                    }
                } else {
                    for channel in channels {
                        if client.channels.remove(&channel) {
                            num_subscribed -= 1;
                        }
                        messages.push(unsubscribe_message(channel, num_subscribed));
                    }
                }

                for message in messages {
                    client.tx.send(message).ok();
                }
            }
            PubSubEvent::Message(channel, message, send_count_tx) => {
                debug!("Sending message to clients for channel {channel:?}: {message:?}");
                let mut send_count = 0;
                for client in pubsub_queue
                    .values()
                    .filter(|client| client.channels.contains(&channel) && !client.tx.is_closed())
                {
                    let message_val = RedisValue::Array(vec![
                        RedisValue::String(Bytes::from_static(b"message")),
                        RedisValue::String(channel.clone()),
                        RedisValue::String(message.clone()),
                    ]);
                    if client.tx.send(message_val).is_ok() {
                        send_count += 1;
                    }
                }

                // Send back the number of clients that the message was successfully sent to
                send_count_tx.send(send_count).ok();
            }
        }
    }
}

fn unsubscribe_message(channel: Bytes, num_subscribed: i64) -> RedisValue {
    RedisValue::Array(vec![
        RedisValue::String(Bytes::from_static(b"unsubscribe")),
        RedisValue::String(channel),
        RedisValue::Int(num_subscribed),
    ])
}
