use std::sync::Arc;

use common::model::repository::datastore::DataStoreRepository;
use redis::ErrorKind;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;

/// Persists queued ambient readings until the daemon cancellation token fires.
#[instrument(level = "info", name = "usecase.log_to_redis", skip_all)]
pub async fn run(
    _config: Arc<Config>,
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

                debug!(operation = "datastore_operation", "Received operation from Nature Remo task");
                match operation {
                        DatastoreOperation::SaveAmbientCondition { ambient_condition } => {
                            match client.save_ambient_condition(ambient_condition).await {
                                Ok(_) => info!(operation = "redis.save_ambient_condition", "Ambient condition saved to Redis"),
                                Err(error) => error!(error = %error, operation = "redis.save_ambient_condition", "Failed to save ambient condition to Redis"),
                            }
                        }
                        DatastoreOperation::FetchAmbientConditions { span, resp, .. } => {
                            let error = redis::RedisError::from((ErrorKind::InvalidClientConfig, "unsupported operation"));
                            span.in_scope(|| {
                                error!(operation = "redis.fetch_ambient_conditions", "Received unsupported operation in templogd Redis worker");
                            });
                            let _ = resp.send(Err(error.into()));
                        }
                        DatastoreOperation::FetchAmbientConditionsWithSampling { span, resp, .. } => {
                            let error = redis::RedisError::from((ErrorKind::InvalidClientConfig, "unsupported operation"));
                            span.in_scope(|| {
                                error!(operation = "redis.fetch_ambient_conditions_with_sampling", "Received unsupported operation in templogd Redis worker");
                            });
                            let _ = resp.send(Err(error.into()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;
    use redis::{RedisError, ToRedisArgs, Value};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tracing::{Event, Subscriber, info_span};
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    };

    #[derive(Clone, Default)]
    struct EventSpanRecorder {
        names: Arc<Mutex<Vec<&'static str>>>,
    }

    impl<S> Layer<S> for EventSpanRecorder
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_event(&self, _event: &Event<'_>, context: Context<'_, S>) {
            if let Some(span) = context.lookup_current() {
                self.names.lock().unwrap().push(span.metadata().name());
            }
        }
    }

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

    fn config() -> Arc<Config> {
        crate::config::new(crate::TemplogdArgs {
            api_token: "".to_string(),
            device_id: "".to_string(),
            redis_host: "".to_string(),
            redis_port: 0,
            log_format: crate::LogFormat::Json,
            log_level: crate::LogLevel::Info,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn saves_ambient_condition_and_stops_on_cancel() {
        let mut datastore = MockDataStore::new();
        datastore.expect_save_ambient_condition().returning(|cond| {
            assert!((cond.get_temperature() - 1.0).abs() < f64::EPSILON);
            Ok(Value::Nil)
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

    #[tokio::test(flavor = "current_thread")]
    async fn stops_when_request_channel_closes() {
        let datastore = MockDataStore::new();
        let (tx, rx) = mpsc::channel(1);
        drop(tx);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run(config(), datastore, rx, CancellationToken::new()),
        )
        .await
        .expect("worker did not stop after the request channel closed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn logs_unsupported_fetches_in_their_request_spans() {
        let recorder = EventSpanRecorder::default();
        let names = recorder.names.clone();
        let subscriber = tracing_subscriber::registry().with(recorder);
        let _guard = tracing::subscriber::set_default(subscriber);

        let datastore = MockDataStore::new();
        let (tx, rx) = mpsc::channel(2);
        let handle = tokio::spawn(run(config(), datastore, rx, CancellationToken::new()));

        let (fetch_tx, fetch_rx) = tokio::sync::oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditions {
            start: "0".into(),
            end: "1".into(),
            span: info_span!("test.fetch_request"),
            resp: fetch_tx,
        })
        .await
        .unwrap();
        assert!(fetch_rx.await.unwrap().is_err());

        let (sampling_tx, sampling_rx) = tokio::sync::oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditionsWithSampling {
            start: "0".into(),
            end: "1".into(),
            samples: "2".into(),
            span: info_span!("test.sampling_request"),
            resp: sampling_tx,
        })
        .await
        .unwrap();
        assert!(sampling_rx.await.unwrap().is_err());

        drop(tx);
        handle.await.unwrap();

        let names = names.lock().unwrap();
        assert!(names.contains(&"test.fetch_request"));
        assert!(names.contains(&"test.sampling_request"));
    }
}
