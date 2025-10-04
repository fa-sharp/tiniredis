mod arguments;
mod command;
mod notifiers;
mod pubsub;
mod queues;
mod server;
mod storage;
mod tasks;
mod transaction;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::start_server().await
}
