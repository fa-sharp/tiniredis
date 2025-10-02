use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

/// Holds the senders/notifiers
pub struct Notifiers {
    pub bpop: mpsc::UnboundedSender<Bytes>,
    pub xread: mpsc::UnboundedSender<Bytes>,
    pub pubsub: mpsc::UnboundedSender<(Bytes, Bytes, oneshot::Sender<i64>)>,
}

impl Notifiers {
    /// Notify blocking pop task that a list was pushed
    pub fn notify_bpop(&self, list_key: Bytes) {
        if self.bpop.send(list_key).is_err() {
            warn!("Blocking pop receiver was dropped");
        }
    }

    /// Notify blocking xread task that an entry was added to a stream
    pub fn notify_xread(&self, stream_key: Bytes) {
        if self.xread.send(stream_key).is_err() {
            warn!("Blocking xread receiver was dropped");
        }
    }

    /// Publish a message to subscribed pubsub clients. The returned receiver
    /// will yield the number of clients that the message was successfully sent to.
    pub fn publish_pubsub(
        &self,
        channel: Bytes,
        message: Bytes,
    ) -> Result<oneshot::Receiver<i64>, mpsc::error::SendError<(Bytes, Bytes, oneshot::Sender<i64>)>>
    {
        let (tx, rx) = oneshot::channel();
        self.pubsub.send((channel, message, tx))?;
        Ok(rx)
    }
}
