use std::fmt::Debug;

use garde::Validate;
use scopeguard::defer;
use tempgrpcd_protos::tempgrpcd::v1::{
    AmbientCondition as ProtoAmbientCondition, GetAmbientConditionsResponse, tempgrpcd_service_server::TempgrpcdService,
};
use tonic::{Response, Status};
use tracing::{debug, info, instrument};

use crate::usecase::port::{GetAmbientConditions, GetAmbientConditionsWithSampling};
use crate::validator::error::ValidationError;
use crate::validator::get_ambient_conditions_request::ValidatedGetAmbientConditionsRequest;

#[derive(Debug)]
pub struct GetAmbientConditionsImpl<G: GetAmbientConditions, S: GetAmbientConditionsWithSampling> {
    get_ambient_conditions_uc: G,
    get_ambient_conditions_with_sampling: S,
}

impl<G: GetAmbientConditions + Debug, S: GetAmbientConditionsWithSampling + Debug> GetAmbientConditionsImpl<G, S> {
    #[instrument(parent = None)]
    pub fn new(get_ambient_conditions_uc: G, get_ambient_conditions_with_sampling: S) -> Self {
        info!("Started");
        defer! {info!("Ended")}

        Self {
            get_ambient_conditions_uc,
            get_ambient_conditions_with_sampling,
        }
    }
}

#[tonic::async_trait]
impl<
    G: GetAmbientConditions + Sync + Send + 'static + Debug,
    S: GetAmbientConditionsWithSampling + Sync + Send + 'static + Debug,
> TempgrpcdService for GetAmbientConditionsImpl<G, S>
{
    #[instrument(parent = None)]
    async fn get_ambient_conditions(
        &self,
        request: tonic::Request<tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest>,
    ) -> Result<Response<GetAmbientConditionsResponse>, Status> {
        debug!("Started");
        defer! {debug!("Ended")}

        let validated_request: ValidatedGetAmbientConditionsRequest<'_> = request.get_ref().into();
        validated_request.validate().map_err(|error| ValidationError::invalid(format!("Validation error: {error}")))?;
        validated_request.validate_business_rules()?;

        let tempgrpcd_request = request.into_inner();
        let start_time = tempgrpcd_request.start_time.unwrap();
        let end_time = tempgrpcd_request.end_time.unwrap();

        let sample = tempgrpcd_request.samples;
        let ambient_conditions = match sample {
            Some(unwrapped_sample) => {
                self.get_ambient_conditions_with_sampling
                    .run(start_time.seconds, end_time.seconds, unwrapped_sample)
                    .await?
            }
            None => {
                // Call the method for getting ambient conditions without sampling
                self.get_ambient_conditions_uc.run(start_time.seconds, end_time.seconds).await?
            }
        };

        Ok(Response::new(GetAmbientConditionsResponse {
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use common::model::ambient_condition::AmbientCondition;
    use pbjson_types::Timestamp;
    use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;
    use tokio::sync::mpsc;
    use tonic::Request;

    use crate::usecase::error::UsecaseError;

    #[derive(Debug)]
    struct StubPrimaryUc {
        tx: mpsc::UnboundedSender<&'static str>,
    }

    #[async_trait::async_trait]
    impl GetAmbientConditions for StubPrimaryUc {
        async fn run(
            &self,
            _start_time_seconds: i64,
            _end_time_seconds: i64,
        ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
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

    #[async_trait::async_trait]
    impl GetAmbientConditionsWithSampling for StubSamplingUc {
        async fn run(
            &self,
            _start_time_seconds: i64,
            _end_time_seconds: i64,
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

    #[tokio::test]
    async fn routes_without_samples_to_primary_uc() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, _rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(StubPrimaryUc { tx: tx_primary }, StubSamplingUc { tx: tx_sampling });

        let resp = svc.get_ambient_conditions(request(false)).await.unwrap().into_inner();
        assert_eq!(rx_primary.recv().await.unwrap(), "called");
        assert_eq!(resp.ambient_conditions["k"].temperature, 1.0);
    }

    #[tokio::test]
    async fn routes_with_samples_to_sampling_uc() {
        let (tx_primary, _rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(StubPrimaryUc { tx: tx_primary }, StubSamplingUc { tx: tx_sampling });

        let resp = svc.get_ambient_conditions(request(true)).await.unwrap().into_inner();
        assert_eq!(rx_sampling.recv().await.unwrap(), "called");
        assert_eq!(resp.ambient_conditions["k"].temperature, 4.0);
    }

    #[tokio::test]
    async fn rejects_missing_start_time_before_routing() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(StubPrimaryUc { tx: tx_primary }, StubSamplingUc { tx: tx_sampling });

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
        let svc = GetAmbientConditionsImpl::new(StubPrimaryUc { tx: tx_primary }, StubSamplingUc { tx: tx_sampling });

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
        let svc = GetAmbientConditionsImpl::new(StubPrimaryUc { tx: tx_primary }, StubSamplingUc { tx: tx_sampling });

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
            async fn run(
                &self,
                _start_time_seconds: i64,
                _end_time_seconds: i64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
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
                _start_time_seconds: i64,
                _end_time_seconds: i64,
                _samples: u64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                panic!("should not be called");
            }
        }

        let svc = GetAmbientConditionsImpl::new(FailingPrimaryUc, NeverCalledSamplingUc);

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
            async fn run(
                &self,
                _start_time_seconds: i64,
                _end_time_seconds: i64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                panic!("should not be called");
            }
        }

        #[derive(Debug)]
        struct FailingSamplingUc;

        #[async_trait::async_trait]
        impl GetAmbientConditionsWithSampling for FailingSamplingUc {
            async fn run(
                &self,
                _start_time_seconds: i64,
                _end_time_seconds: i64,
                _samples: u64,
            ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
                Err(UsecaseError::dependency_unavailable("sampling channel closed"))
            }
        }

        let svc = GetAmbientConditionsImpl::new(NeverCalledPrimaryUc, FailingSamplingUc);

        let err = svc.get_ambient_conditions(request(true)).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(err.message(), "sampling channel closed");
    }
}
