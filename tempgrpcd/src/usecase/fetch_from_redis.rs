use common::model::repository::datastore::DataStoreRepository;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    mut client: impl DataStoreRepository,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    loop {
        tokio::select! {
            operation = rx.recv() => {
                debug!("Received operation from get_ambient_conditions task: {:?}", operation);
                if let Some(operation) = operation {
                    match operation {
                        DatastoreOperation::FetchAmbientConditions { start, end, resp } => {
                            let res = client.fetch_ambient_conditions(start, end).await;
                            info!("Fetched ambient conditions from Redis: {:?}", res);

                            let result = resp.send(res);
                            if let Err(e) = result {
                                    error!("Failed to send ambient conditions to get_ambient_conditions task: {:?}", e);
                            }
                            info!("Sent ambient conditions to get_ambient_conditions task");
                        }
                        DatastoreOperation::FetchAmbientConditionsWithSampling { start, end, samples, resp } => {
                            let res = client.fetch_ambient_conditions_with_sampling(start, end, samples).await;
                            info!("Fetched ambient conditions with sampling from Redis: {:?}", res);

                            let result = resp.send(res);
                            if let Err(e) = result {
                                error!("Failed to send ambient conditions with sampling to get_ambient_conditions task: {:?}", e);
                            }
                            info!("Sent ambient conditions with sampling to get_ambient_conditions task");
                        }
                        DatastoreOperation::SaveAmbientCondition { ambient_condition: _ } => {
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
