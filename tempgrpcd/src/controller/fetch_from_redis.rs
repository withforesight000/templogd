use tracing::{info, instrument};

use crate::model::repository::ambient_condition::AmbientCondition;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    client: impl AmbientCondition,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
) {
    info!("hogehogehogehogehoge");
    usecase::fetch_from_redis::run(client, rx).await;
}
