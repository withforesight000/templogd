use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::{
    controller::get_ambient_conditions::GetAmbientConditions,
    pb::{
        self,
        tempgrpcd::{TempgrpcdRequest, TempgrpcdResponse},
    },
};

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
    async fn run(&self, request: Request<TempgrpcdRequest>) -> Result<Response<TempgrpcdResponse>, Status> {
        debug!("Started");
        defer! {debug!("Ended")}

        let tempgrpcd_request = request.into_inner();
        let start = tempgrpcd_request.start_time;
        let end = tempgrpcd_request.end_time;
        let samples = tempgrpcd_request.samples.unwrap();

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start: start.to_string(),
                end: end.to_string(),
                samples: samples.to_string(),
                resp: resp_tx,
            })
            .await
            .unwrap();
        info!("sent FetchAmbientConditions to fetch_from_redis task");

        let ambient_conditions = resp_rx.await.unwrap().unwrap();
        debug!("Received ambient conditions with sampling: {:?}", ambient_conditions);

        Ok(Response::new(TempgrpcdResponse {
            version: 1,
            ambient_conditions: ambient_conditions
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        pb::tempgrpcd::AmbientCondition {
                            temperature: value.get_temperature() as f32,
                            humidity: value.get_humidity() as f32,
                            illumination: value.get_illumination() as f32,
                        },
                    )
                })
                .collect::<HashMap<String, pb::tempgrpcd::AmbientCondition>>(),
        }))

        // Err(Status::unimplemented("Not implemented"))
    }
}
