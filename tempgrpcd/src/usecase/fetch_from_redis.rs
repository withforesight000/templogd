use tracing::instrument;

use crate::model::repository::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    mut client: impl AmbientCondition,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
) {
    while let Some(operation) = rx.recv().await {
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
}
