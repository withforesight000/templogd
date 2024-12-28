use crate::usecase;
use common::{gateway::interface::redis::Redis, model::channel::datastore_operation::DatastoreOperation};

use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

#[instrument(parent = None, skip(client))]
pub async fn run(
    client: impl Redis,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("hogehogehogehogehoge");
    usecase::fetch_from_redis::run(client, rx, cancellation_token).await;
}
