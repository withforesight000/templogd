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

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;
    use redis::{RedisError, ToRedisArgs, Value};
    use tokio::sync::oneshot;

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
            ) -> Result<Value, RedisError>;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_fetch_and_sampling_requests() {
        let mut datastore = MockDataStore::new();
        datastore.expect_fetch_ambient_conditions().returning(|s: String, e: String| {
            assert_eq!(s, "0");
            assert_eq!(e, "1");
            Ok(std::collections::HashMap::new())
        });
        datastore.expect_fetch_ambient_conditions_with_sampling().returning(|s: String, e: String, samples: String| {
            assert_eq!(s, "0");
            assert_eq!(e, "1");
            assert_eq!(samples, "2");
            Ok(std::collections::HashMap::new())
        });

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let token = CancellationToken::new();

        let run_fut = run(datastore, rx, token.clone());
        let send_and_cancel = async {
            // Fetch without sampling
            let (resp_tx1, resp_rx1) = oneshot::channel();
            tx.send(DatastoreOperation::FetchAmbientConditions {
                start: "0".into(),
                end: "1".into(),
                resp: resp_tx1,
            })
            .await
            .unwrap();

            // Fetch with sampling
            let (resp_tx2, resp_rx2) = oneshot::channel();
            tx.send(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start: "0".into(),
                end: "1".into(),
                samples: "2".into(),
                resp: resp_tx2,
            })
            .await
            .unwrap();

            assert!(resp_rx1.await.unwrap().is_ok());
            assert!(resp_rx2.await.unwrap().is_ok());

            token.cancel();
        };

        let _ = tokio::join!(run_fut, send_and_cancel);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panics_on_save_operation() {
        let datastore = MockDataStore::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();
        let handle = tokio::spawn(run(datastore, rx, token.clone()));

        tx.send(DatastoreOperation::SaveAmbientCondition {
            ambient_condition: ambient_condition::new(1.0, 2.0, 3.0),
        })
        .await
        .unwrap();

        // Expect task to panic when hitting the SaveAmbientCondition arm
        let join = handle.await;
        assert!(join.is_err());
    }
}
