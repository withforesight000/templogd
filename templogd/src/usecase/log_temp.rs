use std::sync::Arc;

use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use crate::config::Config;
use common::model::{channel::datastore_operation::DatastoreOperation, repository::nature_remo::NatureRemo};

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl NatureRemo,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    loop {
        let nature_remo_result = client.fetch_ambient_condition().await;
        let condition = match nature_remo_result {
            Ok(condition) => Some(condition),
            Err(e) => {
                error!("Failed to fetch ambient condition from NatureRemo: {:?}", e);
                None
            }
        };

        if let Some(condition) = condition {
            info!(
                "Got ambient condition from NatureRemo: Temperature: {}, Humidity: {}, Illumination: {}",
                condition.get_temperature(),
                condition.get_humidity(),
                condition.get_illumination()
            );

            info!("Sending ambient condition to log_temp task: {:?}", condition);
            let result = tx
                .send(DatastoreOperation::SaveAmbientCondition {
                    ambient_condition: condition,
                })
                .await;
            match result {
                Ok(_) => info!("Sent ambient condition to log_temp task"),
                Err(e) => error!("Failed to send ambient condition to log_temp task: {:?}", e),
            };
        }

        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {}
            _ = cancellation_token.cancelled() => {
                info!("confirmed cancellation token was cancelled");
                break;
            }
        }
    }
}
