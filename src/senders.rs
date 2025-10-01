use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::warn;

/// Holds the senders/notifiers
pub struct Senders {
    pub bpop: mpsc::UnboundedSender<Bytes>,
    pub xread: mpsc::UnboundedSender<Bytes>,
}

impl Senders {
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
}
