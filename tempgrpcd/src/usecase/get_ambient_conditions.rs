use common::model::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;
use std::collections::HashMap;
use tracing::{Span, debug, info, instrument};

use crate::usecase::error::UsecaseError;
use crate::usecase::port::GetAmbientConditions;

#[derive(Debug)]
pub struct GetAmbientConditionsUC {
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
}

impl GetAmbientConditionsUC {
    /// Creates a use case that sends plain range queries to the Redis worker.
    #[instrument(level = "info", name = "usecase.get_ambient_conditions.new", skip_all)]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Self {
        Self { tx }
    }
}

#[async_trait::async_trait]
impl GetAmbientConditions for GetAmbientConditionsUC {
    /// Queues a range query and waits for the worker's domain result.
    #[instrument(level = "info", name = "usecase.get_ambient_conditions", skip_all, err)]
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditions {
                start: start_time_seconds.to_string(),
                end: end_time_seconds.to_string(),
                span: Span::current(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition request channel closed"))?;
        info!(operation = "redis.fetch_ambient_conditions", "Redis fetch queued");

        let ambient_conditions = resp_rx
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition response channel closed"))??;

        debug!(
            operation = "redis.fetch_ambient_conditions",
            count = ambient_conditions.len(),
            "Redis fetch result received"
        );
        Ok(ambient_conditions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usecase::error::UsecaseError;
    use crate::usecase::get_ambient_conditions_with_sampling::GetAmbientConditionsWithSamplingUC;
    use common::model::repository::datastore::DataStoreError;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tracing::{Id, Subscriber, span::Attributes};
    use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt};

    #[derive(Clone, Default)]
    struct SpanRecorder {
        names: Arc<Mutex<Vec<&'static str>>>,
    }

    impl<S> Layer<S> for SpanRecorder
    where
        S: Subscriber,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _context: Context<'_, S>) {
            self.names.lock().unwrap().push(attrs.metadata().name());
        }
    }

    #[test]
    fn constructors_use_component_specific_span_names() {
        let recorder = SpanRecorder::default();
        let names = recorder.names.clone();
        let subscriber = tracing_subscriber::registry().with(recorder);

        tracing::subscriber::with_default(subscriber, || {
            let (tx, _rx) = mpsc::channel(1);
            let _plain = GetAmbientConditionsUC::new(tx.clone());
            let _sampling = GetAmbientConditionsWithSamplingUC::new(tx);
        });

        let names = names.lock().unwrap();
        assert!(names.contains(&"usecase.get_ambient_conditions.new"));
        assert!(names.contains(&"usecase.get_ambient_conditions_with_sampling.new"));
    }

    #[tokio::test]
    async fn forwards_request_and_maps_response() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        // Spawn receiver to emulate fetch_from_redis
        let handle = tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { start, end, resp, .. }) = rx.recv().await {
                assert_eq!(start, "0");
                assert_eq!(end, "1");
                let mut map = std::collections::HashMap::new();
                map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
                resp.send(Ok(map)).unwrap();
            } else {
                panic!("no operation received");
            }
        });

        let resp = uc.run(0, 1).await.unwrap();
        assert_eq!(resp["k"].get_temperature(), 1.0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn returns_channel_closed_if_request_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        drop(rx);

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition request channel closed")
        );
    }

    #[tokio::test]
    async fn returns_channel_closed_if_response_sender_is_closed() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                drop(resp);
            }
        });

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition response channel closed")
        );
    }

    #[tokio::test]
    async fn returns_storage_error_from_datastore() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                resp.send(Err(DataStoreError::from(redis::RedisError::from((
                    redis::ErrorKind::Io,
                    "redis unavailable",
                )))))
                .unwrap();
            }
        });

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(matches!(
            err,
            UsecaseError::Storage(DataStoreError::Unavailable(error)) if error.to_string().contains("redis unavailable")
        ));
    }
}
