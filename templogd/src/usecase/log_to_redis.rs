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

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;
    use redis::{RedisError, ToRedisArgs};
    use tokio::sync::mpsc;

    mock! {
        pub DataStore {}

        #[async_trait::async_trait]
        impl DataStoreRepository for DataStore {
            async fn fetch_ambient_conditions<
                T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, RedisError>;

            async fn fetch_ambient_conditions_with_sampling<
                T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
                samples: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, RedisError>;

            async fn save_ambient_condition(
                &mut self,
                ambient_condition: ambient_condition::AmbientCondition,
            ) -> Result<redis::Value, RedisError>;
        }
    }

    fn config() -> Arc<Config> {
        crate::config::new(crate::TemplogdArgs {
            api_token: "".to_string(),
            device_id: "".to_string(),
            redis_host: "".to_string(),
            redis_port: 0,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn saves_ambient_condition_and_stops_on_cancel() {
        let mut datastore = MockDataStore::new();
        datastore.expect_save_ambient_condition().returning(|cond| {
            assert!((cond.get_temperature() - 1.0).abs() < f64::EPSILON);
            Ok(redis::Value::Nil)
        });

        let (tx, rx) = mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = run(config(), datastore, rx, token.clone());
        let send_and_cancel = async {
            tx.send(DatastoreOperation::SaveAmbientCondition {
                ambient_condition: ambient_condition::new(1.0, 2.0, 3.0),
            })
            .await
            .unwrap();
            token.cancel();
        };

        let _ = tokio::join!(run_fut, send_and_cancel);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stops_when_cancelled_without_operations() {
        let datastore = MockDataStore::new();
        let (_tx, rx) = mpsc::channel(1);
        let token = CancellationToken::new();

        token.cancel();
        run(config(), datastore, rx, token).await;
    }
}
