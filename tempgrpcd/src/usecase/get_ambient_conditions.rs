use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::pb::{
    self,
    tempgrpcd::{TempgrpcdRequest, TempgrpcdResponse},
};

#[instrument(parent = None)]
pub async fn get_ambient_conditions(
    request: Request<TempgrpcdRequest>,
    tx: &tokio::sync::mpsc::Sender<DatastoreOperation>,
) -> Result<Response<TempgrpcdResponse>, Status> {
    debug!("Started");
    defer! {debug!("Ended")}

    let tempgrpcd_request = request.into_inner();
    let start = tempgrpcd_request.start_time;
    let end = tempgrpcd_request.end_time;

    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(DatastoreOperation::FetchAmbientConditions {
        start: start.to_string(),
        end: end.to_string(),
        resp: resp_tx,
    })
    .await
    .unwrap();
    info!("sent FetchAmbientConditions to fetch_from_redis task");

    let ambient_conditions = resp_rx.await.unwrap().unwrap();

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
