use std::sync::Arc;

use common::gateway::interface::nature_remo::NatureRemo;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl NatureRemo,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::log_temp::run(config, client, tx, cancellation_token).await;
}
