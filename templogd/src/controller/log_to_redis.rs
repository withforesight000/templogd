use std::sync::Arc;

use tracing::instrument;

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;
use common::model::repository::ambient_condition::AmbientCondition;
use crate::usecase;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
) {
    usecase::log_to_redis::run(config, client, rx).await;
}
