use std::fmt::Debug;

use scopeguard::defer;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::pb::tempgrpcd::{tempgrpcd_server::Tempgrpcd, TempgrpcdRequest, TempgrpcdResponse};

#[tonic::async_trait]
pub trait GetAmbientConditions {
    async fn run(&self, request: Request<TempgrpcdRequest>) -> Result<Response<TempgrpcdResponse>, Status>;
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
impl<G: GetAmbientConditions + Sync + Send + 'static + Debug, S: GetAmbientConditions + Sync + Send + 'static + Debug> Tempgrpcd for GetAmbientConditionsImpl<G, S> {
    #[instrument(parent = None)]
    async fn get_ambient_conditions(
        &self,
        request: Request<TempgrpcdRequest>,
    ) -> Result<Response<TempgrpcdResponse>, Status> {
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
