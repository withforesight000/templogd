use std::sync::Arc;

use common::model::repository::datastore::DataStoreRepository;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    _config: Arc<Config>,
    mut client: impl DataStoreRepository,
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
                            let _ =client.save_ambient_condition(ambient_condition).await;
                        }
                        DatastoreOperation::FetchAmbientConditions { start: _, end: _, resp: _ } => {
                            panic!()
                        }
                        DatastoreOperation::FetchAmbientConditionsWithSampling { start: _, end: _, samples: _, resp: _ } => {
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
