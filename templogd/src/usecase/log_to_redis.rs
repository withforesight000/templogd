use std::sync::Arc;

use common::gateway::interface::redis::Redis;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    _config: Arc<Config>,
    mut client: impl Redis,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    loop {
        tokio::select! {
            operation = rx.recv() => {
                debug!("Received operation from log_temp task: {:?}", operation);
                if let Some(operation) = operation {
                    match operation {
                        DatastoreOperation::SaveAmbientCondition { ambient_condition } => {
                            let key = "ambient_condition";
                            let id = "*";
                            let items = vec![
                                ("temperature", ambient_condition.get_temperature()),
                                ("humidity", ambient_condition.get_humidity()),
                                ("illumination", ambient_condition.get_illumination()),
                            ];

                            let res = client.xadd(key, id, items.as_slice()).await;
                            info!("Saved ambient condition to Redis: {:?}", res);
                        }
                        DatastoreOperation::FetchAmbientConditions { start: _, end: _, resp: _ } => {
                            panic!()
                        }
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                info!("confirmed cancellation token was cancelled");
                break;
            }
        }
    }
}
