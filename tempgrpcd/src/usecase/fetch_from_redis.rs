use common::model::repository::datastore::DataStoreRepository;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, instrument};

use common::model::channel::datastore_operation::DatastoreOperation;

/// Receives datastore operations from gRPC use cases and returns Redis results.
///
/// The worker runs until cancellation and converts repository errors into the
/// typed datastore errors carried by the response channels.
#[instrument(level = "info", name = "usecase.fetch_from_redis", skip_all)]
pub async fn run(
    mut client: impl DataStoreRepository,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    loop {
        tokio::select! {
            operation = rx.recv() => {
                let Some(operation) = operation else {
                    info!(reason = "request_channel_closed", "Redis worker stopped");
                    break;
                };

                log_received_operation(&operation);
                match operation {
                    DatastoreOperation::FetchAmbientConditions { start, end, span, resp } => {
                        let res = client
                            .fetch_ambient_conditions(start, end)
                            .instrument(span.clone())
                            .await
                            .map_err(Into::into);
                        span.in_scope(|| {
                            info!(
                                operation = "redis.fetch_ambient_conditions",
                                result = res.is_ok(),
                                "Redis fetch completed"
                            );
                        });

                        if resp.send(res).is_err() {
                            span.in_scope(|| {
                                error!(
                                    operation = "redis.fetch_ambient_conditions",
                                    "Redis fetch result receiver was dropped"
                                );
                            });
                        }
                        span.in_scope(|| {
                            debug!(operation = "redis.fetch_ambient_conditions", "Redis fetch result returned");
                        });
                    }
                    DatastoreOperation::FetchAmbientConditionsWithSampling { start, end, samples, span, resp } => {
                        let res = client
                            .fetch_ambient_conditions_with_sampling(start, end, samples)
                            .instrument(span.clone())
                            .await
                            .map_err(Into::into);
                        span.in_scope(|| {
                            info!(
                                operation = "redis.fetch_ambient_conditions_with_sampling",
                                result = res.is_ok(),
                                "Redis sampling fetch completed"
                            );
                        });

                        if resp.send(res).is_err() {
                            span.in_scope(|| {
                                error!(
                                    operation = "redis.fetch_ambient_conditions_with_sampling",
                                    "Redis sampling result receiver was dropped"
                                );
                            });
                        }
                        span.in_scope(|| {
                            debug!(operation = "redis.fetch_ambient_conditions_with_sampling", "Redis sampling result returned");
                        });
                    }
                    DatastoreOperation::SaveAmbientCondition { ambient_condition: _ } => {
                        error!(operation = "redis.save_ambient_condition", "Received unsupported save operation in tempgrpcd Redis worker");
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                info!(reason = "cancellation_requested", "Redis worker stopped");
                break;
            }
        }
    }
}

/// Logs a received operation while entering its originating request span.
fn log_received_operation(operation: &DatastoreOperation) {
    match operation {
        DatastoreOperation::FetchAmbientConditions { span, .. }
        | DatastoreOperation::FetchAmbientConditionsWithSampling { span, .. } => {
            span.in_scope(|| {
                debug!(
                    operation = "datastore_operation",
                    "Received operation from gRPC request task"
                );
            });
        }
        DatastoreOperation::SaveAmbientCondition { .. } => {
            debug!(
                operation = "datastore_operation",
                "Received operation from gRPC request task"
            );
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
    use tokio_util::sync::CancellationToken;

    mock! {
        pub DataStore {}

        #[async_trait::async_trait]
        impl DataStoreRepository for DataStore {
            async fn fetch_ambient_conditions<
                T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, RedisError>;

            async fn fetch_ambient_conditions_with_sampling<
                T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
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
                span: tracing::Span::current(),
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
                span: tracing::Span::current(),
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
    async fn propagates_datastore_errors_for_fetch_and_sampling_requests() {
        let mut datastore = MockDataStore::new();
        datastore.expect_fetch_ambient_conditions().returning(|s: String, e: String| {
            assert_eq!(s, "0");
            assert_eq!(e, "1");
            Err(RedisError::from((redis::ErrorKind::Io, "fetch failed")))
        });
        datastore.expect_fetch_ambient_conditions_with_sampling().returning(|s: String, e: String, samples: String| {
            assert_eq!(s, "0");
            assert_eq!(e, "1");
            assert_eq!(samples, "2");
            Err(RedisError::from((redis::ErrorKind::Io, "sampling failed")))
        });

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let token = CancellationToken::new();
        let run_fut = tokio::spawn(run(datastore, rx, token.clone()));

        let (resp_tx1, resp_rx1) = oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditions {
            start: "0".into(),
            end: "1".into(),
            span: tracing::Span::current(),
            resp: resp_tx1,
        })
        .await
        .unwrap();
        let fetch_err = resp_rx1.await.unwrap().unwrap_err();
        assert!(matches!(
            fetch_err,
            common::model::repository::datastore::DataStoreError::Unavailable(error) if error.to_string().contains("fetch failed")
        ));

        let (resp_tx2, resp_rx2) = oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditionsWithSampling {
            start: "0".into(),
            end: "1".into(),
            samples: "2".into(),
            span: tracing::Span::current(),
            resp: resp_tx2,
        })
        .await
        .unwrap();
        let sampling_err = resp_rx2.await.unwrap().unwrap_err();
        assert!(matches!(
            sampling_err,
            common::model::repository::datastore::DataStoreError::Unavailable(error) if error.to_string().contains("sampling failed")
        ));

        token.cancel();
        run_fut.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_dropped_response_receivers_without_logging_results() {
        let mut datastore = MockDataStore::new();
        datastore
            .expect_fetch_ambient_conditions()
            .returning(|_: String, _: String| Ok(std::collections::HashMap::new()));
        datastore
            .expect_fetch_ambient_conditions_with_sampling()
            .returning(|_: String, _: String, _: String| Ok(std::collections::HashMap::new()));

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let token = CancellationToken::new();
        let handle = tokio::spawn(run(datastore, rx, token.clone()));

        let (fetch_tx, fetch_rx) = oneshot::channel();
        drop(fetch_rx);
        tx.send(DatastoreOperation::FetchAmbientConditions {
            start: "0".into(),
            end: "1".into(),
            span: tracing::Span::current(),
            resp: fetch_tx,
        })
        .await
        .unwrap();

        let (sampling_tx, sampling_rx) = oneshot::channel();
        drop(sampling_rx);
        tx.send(DatastoreOperation::FetchAmbientConditionsWithSampling {
            start: "0".into(),
            end: "1".into(),
            samples: "2".into(),
            span: tracing::Span::current(),
            resp: sampling_tx,
        })
        .await
        .unwrap();

        drop(tx);
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ignores_save_operation_without_panicking() {
        let datastore = MockDataStore::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();
        let handle = tokio::spawn(run(datastore, rx, token.clone()));

        tx.send(DatastoreOperation::SaveAmbientCondition {
            ambient_condition: ambient_condition::new(1.0, 2.0, 3.0),
        })
        .await
        .unwrap();

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stops_when_request_channel_closes() {
        let datastore = MockDataStore::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run(datastore, rx, CancellationToken::new()),
        )
        .await
        .expect("worker did not stop after the request channel closed");
    }
}
