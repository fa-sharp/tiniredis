use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::warn;

/// Holds the senders/notifiers
pub struct Senders {
    pub bpop: mpsc::UnboundedSender<Bytes>,
}

impl Senders {
    /// Notify blocking pop task that a list was pushed
    pub fn notify_bpop(&self, bytes: Bytes) {
        if let Err(_) = self.bpop.send(bytes) {
            warn!("Blocking pop receiver was dropped");
        }
    }
}
