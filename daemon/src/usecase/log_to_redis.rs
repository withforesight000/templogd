use std::sync::Arc;

use tracing::{info, instrument};

use crate::config::Config;
use crate::model::channel::datastore_operation::DatastoreOperation;
use crate::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None, skip(client))]
pub async fn run(
    _config: Arc<Config>,
    mut client: impl AmbientCondition,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
) {
    while let Some(operation) = rx.recv().await {
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
        }
    }
}
