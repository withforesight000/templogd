use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;
use common::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    loop {
        let condition = client.fetch_current_ambient_condition().await.unwrap();
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
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {}
            _ = cancellation_token.cancelled() => {
                info!("confirmed cancellation token was cancelled");
                break;
            }
        }
    }
}
