use std::sync::Arc;

use common::model::repository::datastore::DataStoreRepository;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl DataStoreRepository,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    usecase::log_to_redis::run(config, client, rx, cancellation_token).await;
}
