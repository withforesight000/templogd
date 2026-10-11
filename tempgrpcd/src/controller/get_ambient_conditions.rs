use std::{fmt::Debug, sync::Arc, time::Duration};

use garde::Validate;
use tempgrpcd_protos::tempgrpcd::v1::{
    AmbientCondition as ProtoAmbientCondition, GetAmbientConditionsResponse, tempgrpcd_service_server::TempgrpcdService,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use tracing::{Span, field, instrument, warn};

use crate::usecase::port::{GetAmbientConditions, GetAmbientConditionsWithSampling, QueryContext};
use crate::validator::error::ValidationError;
use crate::validator::get_ambient_conditions_request::ValidatedGetAmbientConditionsRequest;

/// Validates gRPC requests, selects a use case, and maps domain readings to protobuf values.
#[derive(Debug)]
pub struct GetAmbientConditionsImpl<G: GetAmbientConditions, S: GetAmbientConditionsWithSampling> {
    get_ambient_conditions_uc: G,
    get_ambient_conditions_with_sampling: S,
    max_samples: u64,
    admission: Arc<Semaphore>,
    request_timeout: Duration,
    accept_legacy_millisecond_timestamps: bool,
}

impl<G: GetAmbientConditions + Debug, S: GetAmbientConditionsWithSampling + Debug> GetAmbientConditionsImpl<G, S> {
    /// Creates a controller with plain and sampled ambient-condition use cases.
    #[instrument(level = "info", name = "controller.get_ambient_conditions.new", skip_all)]
    pub fn new(
        get_ambient_conditions_uc: G,
        get_ambient_conditions_with_sampling: S,
        max_samples: u64,
        max_concurrent_requests: usize,
        request_timeout: Duration,
        accept_legacy_millisecond_timestamps: bool,
    ) -> Self {
        Self {
            get_ambient_conditions_uc,
            get_ambient_conditions_with_sampling,
            max_samples,
            admission: Arc::new(Semaphore::new(max_concurrent_requests)),
            request_timeout,
            accept_legacy_millisecond_timestamps,
        }
    }
}

#[tonic::async_trait]
impl<
    G: GetAmbientConditions + Sync + Send + 'static + Debug,
    S: GetAmbientConditionsWithSampling + Sync + Send + 'static + Debug,
> TempgrpcdService for GetAmbientConditionsImpl<G, S>
{
    #[instrument(
        level = "info",
        name = "controller.get_ambient_conditions",
        skip_all,
        fields(samples = field::Empty, route = field::Empty),
        err
    )]
    async fn get_ambient_conditions(
        &self,
        request: tonic::Request<tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest>,
    ) -> Result<Response<GetAmbientConditionsResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.request_timeout;
        let cancellation = CancellationToken::new();
        let _cancellation_guard = RequestCancellationGuard(cancellation.clone());
        let samples = request.get_ref().samples;
        Span::current().record("samples", field::debug(samples));
        Span::current().record("route", if samples.is_some() { "sampling" } else { "plain" });
        let validated_request: ValidatedGetAmbientConditionsRequest<'_> = request.get_ref().into();
        validated_request.validate().map_err(|error| ValidationError::invalid(format!("Validation error: {error}")))?;
        validated_request.validate_business_rules(self.max_samples)?;
        let redis_range = validated_request.to_redis_time_range(self.accept_legacy_millisecond_timestamps)?;
        if redis_range.used_legacy_encoding {
            warn!(
                reason = "legacy_timestamp_encoding",
                "Accepted legacy roomtemp timestamp encoding"
            );
        }
        ensure_before_deadline(deadline)?;

        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many concurrent ambient condition requests"))?;

        let tempgrpcd_request = request.into_inner();
        let sample = tempgrpcd_request.samples;
        let context = QueryContext {
            start: redis_range.start,
            end: redis_range.end,
            deadline,
            cancellation,
        };
        let ambient_conditions = tokio::time::timeout_at(deadline, async {
            match sample {
                Some(unwrapped_sample) => {
                    self.get_ambient_conditions_with_sampling.run(context, unwrapped_sample).await
                }
                None => self.get_ambient_conditions_uc.run(context).await,
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("request deadline exceeded"))??;

        let response = Response::new(GetAmbientConditionsResponse {
            ambient_conditions: ambient_conditions
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        ProtoAmbientCondition {
                            temperature: value.get_temperature() as f32,
                            humidity: value.get_humidity() as f32,
                            illumination: value.get_illumination() as f32,
                        },
                    )
                })
                .collect(),
        });
        ensure_before_deadline(deadline)?;
        Ok(response)
    }
}

fn ensure_before_deadline(deadline: tokio::time::Instant) -> Result<(), Status> {
    if tokio::time::Instant::now() >= deadline {
        return Err(Status::deadline_exceeded("request deadline exceeded"));
    }
    Ok(())
}

struct RequestCancellationGuard(CancellationToken);

impl Drop for RequestCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use common::model::ambient_condition::AmbientCondition;
    use pbjson_types::Timestamp;
    use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;
    use tokio::sync::mpsc;
    use tonic::Request;
    use tracing::{Id, Subscriber, span::Attributes};
    use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt};

    use crate::usecase::error::UsecaseError;

    #[derive(Debug)]
    struct StubPrimaryUc {
        tx: mpsc::UnboundedSender<&'static str>,
    }

    #[async_trait::async_trait]
    impl GetAmbientConditions for StubPrimaryUc {
        async fn run(&self, _context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
            let _ = self.tx.send("called");
            let mut map = HashMap::new();
            map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
            Ok(map)
        }
    }

    #[derive(Debug)]
    struct StubSamplingUc {
        tx: mpsc::UnboundedSender<&'static str>,
    }

    #[derive(Debug)]
    struct StubDelayedPrimaryUc {
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl GetAmbientConditions for StubDelayedPrimaryUc {
        async fn run(&self, _context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
            std::thread::sleep(self.delay);
            let mut map = HashMap::new();
            map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
            Ok(map)
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

    #[async_trait::async_trait]
    impl GetAmbientConditionsWithSampling for StubSamplingUc {
        async fn run(
            &self,
            _context: QueryContext,
            _samples: u64,
        ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
            let _ = self.tx.send("called");
            let mut map = HashMap::new();
            map.insert("k".into(), common::model::ambient_condition::new(4.0, 5.0, 6.0));
            Ok(map)
        }
    }

    fn request(with_samples: bool) -> Request<GetAmbientConditionsRequest> {
        Request::new(GetAmbientConditionsRequest {
            start_time: Some(Timestamp { seconds: 0, nanos: 0 }),
            end_time: Some(Timestamp { seconds: 1, nanos: 0 }),
            samples: if with_samples { Some(1) } else { None },
        })
    }

    fn request_with_fields(
        start_time: Option<Timestamp>,
        end_time: Option<Timestamp>,
        samples: Option<u64>,
    ) -> Request<GetAmbientConditionsRequest> {
        Request::new(GetAmbientConditionsRequest {
            start_time,
            end_time,
            samples,
        })
    }

    #[test]
    fn constructor_uses_a_component_specific_span_name() {
        let recorder = SpanRecorder::default();
        let names = recorder.names.clone();
        let subscriber = tracing_subscriber::registry().with(recorder);

        tracing::subscriber::with_default(subscriber, || {
            // Other parallel tests can rebuild tracing's process-wide callsite cache.
            for _ in 0..100 {
                tracing::callsite::rebuild_interest_cache();
                let (tx_primary, _rx_primary) = mpsc::unbounded_channel();
                let (tx_sampling, _rx_sampling) = mpsc::unbounded_channel();
                let _svc = GetAmbientConditionsImpl::new(
                    StubPrimaryUc { tx: tx_primary },
                    StubSamplingUc { tx: tx_sampling },
                    1000,
                    4,
                    Duration::from_secs(1),
                    true,
                );
                if names.lock().unwrap().contains(&"controller.get_ambient_conditions.new") {
                    break;
                }
            }
        });

        assert!(names.lock().unwrap().contains(&"controller.get_ambient_conditions.new"));
    }

    #[tokio::test]
    async fn routes_without_samples_to_primary_uc() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, _rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubPrimaryUc { tx: tx_primary },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let resp = svc.get_ambient_conditions(request(false)).await.unwrap().into_inner();
        assert_eq!(rx_primary.recv().await.unwrap(), "called");
        assert_eq!(resp.ambient_conditions["k"].temperature, 1.0);
    }

    #[tokio::test]
    async fn rejects_responses_completed_after_the_rpc_deadline() {
        let (tx_sampling, _rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubDelayedPrimaryUc {
                delay: Duration::from_millis(20),
            },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_millis(1),
            true,
        );

        let status = svc.get_ambient_conditions(request(false)).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn routes_with_samples_to_sampling_uc() {
        let (tx_primary, _rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubPrimaryUc { tx: tx_primary },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let resp = svc.get_ambient_conditions(request(true)).await.unwrap().into_inner();
        assert_eq!(rx_sampling.recv().await.unwrap(), "called");
        assert_eq!(resp.ambient_conditions["k"].temperature, 4.0);
    }

    #[tokio::test]
    async fn rejects_missing_start_time_before_routing() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubPrimaryUc { tx: tx_primary },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let err = svc
            .get_ambient_conditions(request_with_fields(
                None,
                Some(Timestamp { seconds: 1, nanos: 0 }),
                None,
            ))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(rx_primary.try_recv().is_err());
        assert!(rx_sampling.try_recv().is_err());
    }

    #[tokio::test]
    async fn rejects_reversed_time_range_before_routing() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubPrimaryUc { tx: tx_primary },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let err = svc
            .get_ambient_conditions(request_with_fields(
                Some(Timestamp { seconds: 2, nanos: 0 }),
                Some(Timestamp { seconds: 1, nanos: 0 }),
                None,
            ))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(rx_primary.try_recv().is_err());
        assert!(rx_sampling.try_recv().is_err());
    }

    #[tokio::test]
    async fn rejects_zero_samples_before_routing() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubPrimaryUc { tx: tx_primary },
            StubSamplingUc { tx: tx_sampling },
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let err = svc
            .get_ambient_conditions(request_with_fields(
                Some(Timestamp { seconds: 0, nanos: 0 }),
                Some(Timestamp { seconds: 1, nanos: 0 }),
                Some(0),
            ))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(rx_primary.try_recv().is_err());
        assert!(rx_sampling.try_recv().is_err());
    }

    #[tokio::test]
    async fn maps_usecase_error_to_status() {
        #[derive(Debug)]
        struct FailingPrimaryUc;

        #[async_trait::async_trait]
        impl GetAmbientConditions for FailingPrimaryUc {
            async fn run(&self, _context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                Err(UsecaseError::dependency_unavailable(
                    "ambient condition request channel closed",
                ))
            }
        }

        #[derive(Debug)]
        struct NeverCalledSamplingUc;

        #[async_trait::async_trait]
        impl GetAmbientConditionsWithSampling for NeverCalledSamplingUc {
            async fn run(
                &self,
                _context: QueryContext,
                _samples: u64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                panic!("should not be called");
            }
        }

        let svc = GetAmbientConditionsImpl::new(
            FailingPrimaryUc,
            NeverCalledSamplingUc,
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let err = svc.get_ambient_conditions(request(false)).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(err.message(), "ambient condition request channel closed");
    }

    #[tokio::test]
    async fn maps_sampling_usecase_error_to_status() {
        #[derive(Debug)]
        struct NeverCalledPrimaryUc;

        #[async_trait::async_trait]
        impl GetAmbientConditions for NeverCalledPrimaryUc {
            async fn run(&self, _context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                panic!("should not be called");
            }
        }

        #[derive(Debug)]
        struct FailingSamplingUc;

        #[async_trait::async_trait]
        impl GetAmbientConditionsWithSampling for FailingSamplingUc {
            async fn run(
                &self,
                _context: QueryContext,
                _samples: u64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                Err(UsecaseError::dependency_unavailable("sampling channel closed"))
            }
        }

        let svc = GetAmbientConditionsImpl::new(
            NeverCalledPrimaryUc,
            FailingSamplingUc,
            1000,
            4,
            Duration::from_secs(1),
            true,
        );

        let err = svc.get_ambient_conditions(request(true)).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(err.message(), "sampling channel closed");
    }

    #[tokio::test]
    async fn rejects_work_above_the_global_concurrency_limit() {
        #[derive(Debug)]
        struct BlockingPrimaryUc {
            started: mpsc::UnboundedSender<()>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait::async_trait]
        impl GetAmbientConditions for BlockingPrimaryUc {
            async fn run(&self, _context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                let _ = self.started.send(());
                self.release.notified().await;
                Ok(HashMap::new())
            }
        }

        #[derive(Debug)]
        struct UnusedSamplingUc;

        #[async_trait::async_trait]
        impl GetAmbientConditionsWithSampling for UnusedSamplingUc {
            async fn run(
                &self,
                _context: QueryContext,
                _samples: u64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                panic!("sampling should not be called")
            }
        }

        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let service = Arc::new(GetAmbientConditionsImpl::new(
            BlockingPrimaryUc {
                started: started_tx,
                release: release.clone(),
            },
            UnusedSamplingUc,
            1000,
            1,
            Duration::from_secs(1),
            false,
        ));
        let first_service = service.clone();
        let first = tokio::spawn(async move { first_service.get_ambient_conditions(request(false)).await });
        started_rx.recv().await.unwrap();

        let error = service.get_ambient_conditions(request(false)).await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        release.notify_one();
        assert!(first.await.unwrap().is_ok());
    }
}
