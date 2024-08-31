use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;
use common::model::repository::ambient_condition::AmbientCondition;
use crate::usecase;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::log_temp::run(config, client, tx, cancellation_token).await;
}
