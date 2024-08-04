use tracing::{info, instrument};

use crate::gateway::ambient_condition::AmbientConditionRepository;
use common::infra::redis_client::AsyncRedisCrateClient;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    client: AmbientConditionRepository<AsyncRedisCrateClient>,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
) {
    info!("hogehogehogehogehoge");
    usecase::fetch_from_redis::run(client, rx).await;
}
