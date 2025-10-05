use std::sync::atomic::{self, AtomicUsize};

/// Tracks the number of changes since the last database save
#[derive(Debug, Default)]
pub struct ChangeCounter {
    changes: AtomicUsize,
}

impl ChangeCounter {
    /// Increment the data change counter
    pub(super) fn incr(&self, count: usize) {
        self.changes.fetch_add(count, atomic::Ordering::Relaxed);
    }
    /// Load the change count
    pub(super) fn load(&self) -> usize {
        self.changes.load(atomic::Ordering::Relaxed)
    }
    /// Reset the data change counter
    pub(super) fn reset(&self) {
        self.changes.store(0, atomic::Ordering::Relaxed);
    }
}
