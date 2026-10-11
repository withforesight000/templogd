use common::model::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;
use std::collections::HashMap;
use tracing::{Span, debug, info, instrument};

use crate::usecase::error::UsecaseError;
use crate::usecase::port::{GetAmbientConditions, QueryContext};

#[derive(Debug)]
pub struct GetAmbientConditionsUC {
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    max_entries: usize,
}

impl GetAmbientConditionsUC {
    /// Creates a use case that sends plain range queries to the Redis worker.
    #[instrument(level = "info", name = "usecase.get_ambient_conditions.new", skip_all)]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>, max_entries: usize) -> Self {
        Self { tx, max_entries }
    }
}

#[async_trait::async_trait]
impl GetAmbientConditions for GetAmbientConditionsUC {
    /// Queues a range query and waits for the worker's domain result.
    #[instrument(level = "info", name = "usecase.get_ambient_conditions", skip_all, err)]
    async fn run(&self, context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let operation = DatastoreOperation::FetchAmbientConditions {
            start: context.start,
            end: context.end,
            max_entries: self.max_entries,
            deadline: context.deadline,
            cancellation: context.cancellation.clone(),
            span: Span::current(),
            resp: resp_tx,
        };
        match self.tx.try_send(operation) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(UsecaseError::resource_exhausted(
                    "ambient condition request queue is full",
                ));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(UsecaseError::dependency_unavailable(
                    "ambient condition request channel closed",
                ));
            }
        }
        info!(operation = "redis.fetch_ambient_conditions", "Redis fetch queued");

        let ambient_conditions = tokio::select! {
            response = resp_rx => response
                .map_err(|_| UsecaseError::dependency_unavailable("ambient condition response channel closed"))??,
            _ = tokio::time::sleep_until(context.deadline) => return Err(UsecaseError::DeadlineExceeded),
            _ = context.cancellation.cancelled() => return Err(UsecaseError::Cancelled),
        };

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

    fn context() -> QueryContext {
        QueryContext {
            start: "0-0".into(),
            end: "1-18446744073709551615".into(),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

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
            let _plain = GetAmbientConditionsUC::new(tx.clone(), 5000);
            let _sampling = GetAmbientConditionsWithSamplingUC::new(tx);
        });

        let names = names.lock().unwrap();
        assert!(names.contains(&"usecase.get_ambient_conditions.new"));
        assert!(names.contains(&"usecase.get_ambient_conditions_with_sampling.new"));
    }

    #[tokio::test]
    async fn forwards_request_and_maps_response() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx, 5000);

        // Spawn receiver to emulate fetch_from_redis
        let handle = tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { start, end, resp, .. }) = rx.recv().await {
                assert_eq!(start, "0-0");
                assert_eq!(end, "1-18446744073709551615");
                let mut map = std::collections::HashMap::new();
                map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
                resp.send(Ok(map)).unwrap();
            } else {
                panic!("no operation received");
            }
        });

        let resp = uc.run(context()).await.unwrap();
        assert_eq!(resp["k"].get_temperature(), 1.0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn returns_channel_closed_if_request_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx, 5000);

        drop(rx);

        let err = uc.run(context()).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition request channel closed")
        );
    }

    #[tokio::test]
    async fn returns_channel_closed_if_response_sender_is_closed() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx, 5000);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                drop(resp);
            }
        });

        let err = uc.run(context()).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition response channel closed")
        );
    }

    #[tokio::test]
    async fn returns_storage_error_from_datastore() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx, 5000);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                resp.send(Err(DataStoreError::from(redis::RedisError::from((
                    redis::ErrorKind::Io,
                    "redis unavailable",
                )))))
                .unwrap();
            }
        });

        let err = uc.run(context()).await.unwrap_err();

        assert!(matches!(
            err,
            UsecaseError::Storage(DataStoreError::Unavailable(error)) if error.to_string().contains("redis unavailable")
        ));
    }
}
