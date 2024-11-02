use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;
use common::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::log_to_redis::run(config, client, rx, cancellation_token).await;
}
