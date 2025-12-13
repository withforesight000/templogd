use std::fmt::Debug;

use scopeguard::defer;
use tempgrpcd_protos::tempgrpcd::v1::{
    tempgrpcd_service_server::TempgrpcdService, GetAmbientConditionsRequest, GetAmbientConditionsResponse,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

#[tonic::async_trait]
pub trait GetAmbientConditions {
    async fn run(
        &self,
        request: Request<GetAmbientConditionsRequest>,
    ) -> Result<Response<GetAmbientConditionsResponse>, Status>;
}

#[derive(Debug)]
pub struct GetAmbientConditionsImpl<G: GetAmbientConditions, S: GetAmbientConditions> {
    get_ambient_conditions_uc: G,
    get_ambient_conditions_with_sampling: S,
}

impl<G: GetAmbientConditions + Debug, S: GetAmbientConditions + Debug> GetAmbientConditionsImpl<G, S> {
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
        S: GetAmbientConditions + Sync + Send + 'static + Debug,
    > TempgrpcdService for GetAmbientConditionsImpl<G, S>
{
    #[instrument(parent = None)]
    async fn get_ambient_conditions(
        &self,
        request: tonic::Request<tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest>,
    ) -> Result<Response<GetAmbientConditionsResponse>, Status> {
        debug!("Started");
        defer! {debug!("Ended")}

        let samples = request.get_ref().samples;
        match samples {
            Some(_) => self.get_ambient_conditions_with_sampling.run(request).await,
            None => {
                // Call the method for getting ambient conditions without sampling
                self.get_ambient_conditions_uc.run(request).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pbjson_types::Timestamp;
    use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;
    use tokio::sync::mpsc;
    use tonic::Request;

    #[derive(Debug)]
    struct StubUc {
        tx: mpsc::UnboundedSender<&'static str>,
    }

    #[tonic::async_trait]
    impl GetAmbientConditions for StubUc {
        async fn run(
            &self,
            _request: Request<GetAmbientConditionsRequest>,
        ) -> Result<Response<GetAmbientConditionsResponse>, Status> {
            let _ = self.tx.send("called");
            Ok(Response::new(GetAmbientConditionsResponse { ambient_conditions: Default::default() }))
        }
    }

    fn request(with_samples: bool) -> Request<GetAmbientConditionsRequest> {
        Request::new(GetAmbientConditionsRequest {
            start_time: Some(Timestamp { seconds: 0, nanos: 0 }),
            end_time: Some(Timestamp { seconds: 1, nanos: 0 }),
            samples: if with_samples { Some(1) } else { None },
        })
    }

    #[tokio::test]
    async fn routes_without_samples_to_primary_uc() {
        let (tx_primary, mut rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, _rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubUc { tx: tx_primary },
            StubUc { tx: tx_sampling },
        );

        let _ = svc.get_ambient_conditions(request(false)).await.unwrap();
        assert_eq!(rx_primary.recv().await.unwrap(), "called");
    }

    #[tokio::test]
    async fn routes_with_samples_to_sampling_uc() {
        let (tx_primary, _rx_primary) = mpsc::unbounded_channel();
        let (tx_sampling, mut rx_sampling) = mpsc::unbounded_channel();
        let svc = GetAmbientConditionsImpl::new(
            StubUc { tx: tx_primary },
            StubUc { tx: tx_sampling },
        );

        let _ = svc.get_ambient_conditions(request(true)).await.unwrap();
        assert_eq!(rx_sampling.recv().await.unwrap(), "called");
    }
}
