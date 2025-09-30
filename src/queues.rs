use std::{collections::VecDeque, sync::Mutex};

use crate::tasks::BPopClient;

/// Holds the queues for blocking operations, pub/sub, etc.
#[derive(Debug, Default)]
pub struct Queues {
    bpop: Mutex<VecDeque<BPopClient>>,
}

impl Queues {
    /// Get an exclusive lock on the blocking pop queue
    pub fn bpop_lock(&self) -> std::sync::MutexGuard<'_, VecDeque<BPopClient>> {
        self.bpop.lock().unwrap()
    }
}
