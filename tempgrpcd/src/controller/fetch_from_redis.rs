use crate::usecase;
use common::model::{channel::datastore_operation::DatastoreOperation, repository::datastore::DataStoreRepository};

use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

#[instrument(parent = None, skip(client))]
pub async fn run(
    client: impl DataStoreRepository,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    usecase::fetch_from_redis::run(client, rx, cancellation_token).await;
}
