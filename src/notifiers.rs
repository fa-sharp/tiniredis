use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::tasks::PubSubEvent;

/// Holds the notifiers/senders for various events
pub struct Notifiers {
    pub bpop: mpsc::UnboundedSender<Bytes>,
    pub xread: mpsc::UnboundedSender<Bytes>,
    pub pubsub: mpsc::UnboundedSender<PubSubEvent>,
}

impl Notifiers {
    /// Notify blocking pop task that a list was pushed
    pub fn bpop_notify(&self, list_key: Bytes) {
        if self.bpop.send(list_key).is_err() {
            warn!("Blocking pop receiver was dropped");
        }
    }

    /// Notify blocking xread task that an entry was added to a stream
    pub fn xread_notify(&self, stream_key: Bytes) {
        if self.xread.send(stream_key).is_err() {
            warn!("Blocking xread receiver was dropped");
        }
    }

    /// Publish a message to subscribed pubsub clients. The returned receiver
    /// will yield the number of clients that the message was successfully sent to.
    pub fn pubsub_publish(
        &self,
        channel: Bytes,
        message: Bytes,
    ) -> Result<oneshot::Receiver<i64>, mpsc::error::SendError<PubSubEvent>> {
        let (tx, rx) = oneshot::channel();
        self.pubsub
            .send(PubSubEvent::Message(channel, message, tx))?;
        Ok(rx)
    }

    /// Ping a pubsub client
    pub fn pubsub_ping(&self, id: u64) -> Result<(), mpsc::error::SendError<PubSubEvent>> {
        self.pubsub.send(PubSubEvent::Ping(id))
    }

    /// Subscribe a client to given channel(s)
    pub fn pubsub_subscribe(
        &self,
        id: u64,
        channels: Vec<Bytes>,
    ) -> Result<(), mpsc::error::SendError<PubSubEvent>> {
        self.pubsub.send(PubSubEvent::Subscribe(id, channels))
    }

    /// Unsubscribe a client from given channel(s), or all channels if empty vector
    pub fn pubsub_unsubscribe(
        &self,
        id: u64,
        channels: Vec<Bytes>,
    ) -> Result<(), mpsc::error::SendError<PubSubEvent>> {
        self.pubsub.send(PubSubEvent::Unsubscribe(id, channels))
    }
}
