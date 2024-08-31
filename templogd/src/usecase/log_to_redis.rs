use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;
use common::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None, skip(client))]
pub async fn run(
    _config: Arc<Config>,
    mut client: impl AmbientCondition,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    loop {
        tokio::select! {
            operation = rx.recv() => {
                if let Some(operation) = operation {
                    match operation {
                        DatastoreOperation::SaveAmbientCondition { ambient_condition } => {
                            // let items_ref: Vec<(&str, &str)> = items
                            //     .iter()
                            //     .map(|(k, v)| (k.as_str(), v.as_str()))
                            //     .collect();
                            // let items_slice: &[(&str, &str)] = items_ref.as_slice();
                            let res = client.save_ambient_condition(ambient_condition).await.unwrap();
                            // let res = client.xadd(&key, &id, items_slice).await.unwrap();
                            info!(":::Result from redis: {:?}", res)
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
