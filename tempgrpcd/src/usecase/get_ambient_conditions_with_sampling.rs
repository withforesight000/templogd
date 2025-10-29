use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use std::collections::HashMap;
use tempgrpcd_protos::tempgrpcd::v1::{GetAmbientConditionsRequest, GetAmbientConditionsResponse};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::controller::get_ambient_conditions::GetAmbientConditions;

#[derive(Debug)]
pub struct GetAmbientConditionsWithSamplingUC {
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
}

impl GetAmbientConditionsWithSamplingUC {
    #[instrument(parent = None)]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Self {
        info!("Started");
        defer! {info!("Ended")}

        Self { tx }
    }
}

#[tonic::async_trait]
impl GetAmbientConditions for GetAmbientConditionsWithSamplingUC {
    #[instrument(parent = None)]
    async fn run(
        &self,
        request: Request<GetAmbientConditionsRequest>,
    ) -> Result<Response<GetAmbientConditionsResponse>, Status> {
        debug!("Started");
        defer! {debug!("Ended")}

        let tempgrpcd_request = request.into_inner();
        let start = tempgrpcd_request.start_time;
        let end = tempgrpcd_request.end_time;
        let samples = tempgrpcd_request.samples.unwrap();

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start: start.unwrap().seconds,
                end: end.unwrap().seconds,
                samples,
                resp: resp_tx,
            })
            .await
            .unwrap();
        info!("sent FetchAmbientConditions to fetch_from_redis task");

        let ambient_conditions = resp_rx.await.unwrap().unwrap();
        debug!("Received ambient conditions with sampling: {:?}", ambient_conditions);

        Ok(Response::new(GetAmbientConditionsResponse {
            ambient_conditions: ambient_conditions
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        tempgrpcd_protos::tempgrpcd::v1::AmbientCondition {
                            temperature: value.get_temperature() as f32,
                            humidity: value.get_humidity() as f32,
                            illumination: value.get_illumination() as f32,
                        },
                    )
                })
                .collect::<HashMap<String, tempgrpcd_protos::tempgrpcd::v1::AmbientCondition>>(),
        }))

        // Err(Status::unimplemented("Not implemented"))
    }
}
