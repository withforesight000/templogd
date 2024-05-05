use std::sync::Arc;

use tracing::{info, instrument};

use crate::config::Config;
use crate::model::channel::datastore_operation::DatastoreOperation;
use crate::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
) {
    loop {
        let condition = client.fetch_ambient_condition().await.unwrap();
        info!(
            "Temperature: {}, Humidity: {}, Illumination: {}",
            condition.get_temperature(),
            condition.get_humidity(),
            condition.get_illumination()
        );
        tx.send(DatastoreOperation::SaveAmbientCondition {
            ambient_condition: condition,
        })
        .await
        .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}
