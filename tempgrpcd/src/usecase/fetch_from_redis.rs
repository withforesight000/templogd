use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use common::model::repository::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    mut client: impl AmbientCondition,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    loop {
        tokio::select! {
            operation = rx.recv() => {
                if let Some(operation) = operation {
                    match operation {
                        DatastoreOperation::FetchAmbientConditions { start, end, resp } => {
                            let res = client
                                .fetch_ambient_conditions_between_start_and_end(start, end)
                                .await;
                            resp.send(res).unwrap();
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
