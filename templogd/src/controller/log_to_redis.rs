use std::sync::Arc;

use common::gateway::interface::redis::Redis;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl Redis,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::log_to_redis::run(config, client, rx, cancellation_token).await;
}
