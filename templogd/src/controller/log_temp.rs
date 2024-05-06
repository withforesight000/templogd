use std::sync::Arc;

use tracing::instrument;

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;
use crate::model::repository::ambient_condition::AmbientCondition;
use crate::usecase;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
) {
    usecase::log_temp::run(config, client, tx).await;
}
