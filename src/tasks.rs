use std::sync::{Arc, Mutex};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

mod bpop;
mod cleanup;
mod counters;
mod notifiers;
mod persist;
mod pubsub;
mod queues;
mod xread;

pub use {notifiers::Notifiers, queues::Queues};

use crate::{server::Config, storage::MemoryStorage, tasks::counters::ChangeCounter};

/// Start all server tasks and return task handles, queues, and notifiers
pub fn spawn_server_tasks(
    storage: &Arc<Mutex<MemoryStorage>>,
    config: &Arc<Config>,
    shutdown_sig: &watch::Receiver<bool>,
) -> (JoinSet<()>, Arc<Queues>, Arc<Notifiers>) {
    // Setup channels
    let (bpop_tx, bpop_rx) = mpsc::unbounded_channel();
    let (xread_tx, xread_rx) = mpsc::unbounded_channel();
    let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

    // Setup queues, counters, and notifiers
    let queues: Arc<Queues> = Arc::default();
    let counters: Arc<ChangeCounter> = Arc::default();
    let notifiers: Arc<Notifiers> = Arc::new(Notifiers {
        bpop: bpop_tx,
        xread: xread_tx,
        pubsub: pubsub_tx,
        counters: Arc::clone(&counters),
    });

    // Spawn tasks
    let mut all_tasks = JoinSet::new();
    all_tasks.spawn(bpop::bpop_task(
        Arc::clone(storage),
        Arc::clone(&queues),
        bpop_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(xread::xread_task(
        Arc::clone(storage),
        Arc::clone(&queues),
        xread_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(pubsub::pubsub_task(
        Arc::clone(&queues),
        pubsub_rx,
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(cleanup::cleanup_task(
        Arc::clone(storage),
        Arc::clone(&queues),
        shutdown_sig.clone(),
    ));
    all_tasks.spawn(persist::persist_task(
        Arc::clone(storage),
        Arc::clone(&counters),
        Arc::clone(config),
        shutdown_sig.clone(),
    ));

    (all_tasks, queues, notifiers)
}
