use common::model::channel::datastore_operation::DatastoreOperation;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

use crate::pb::{
    self,
    tempgrpcd::{TempgrpcdRequest, TempgrpcdResponse},
};

#[instrument]
pub async fn get_ambient_conditions(
    request: Request<TempgrpcdRequest>,
    tx: &tokio::sync::mpsc::Sender<DatastoreOperation>,
) -> Result<Response<TempgrpcdResponse>, Status> {
    let tempgrpcd_request = request.into_inner();
    let start = tempgrpcd_request.start_time;
    let end = tempgrpcd_request.end_time;

    // let ambient_conditions = data_store
    //     .lock()
    //     .await
    //     .fetch_ambient_conditions_between_start_and_end(start, end)
    //     .await
    //     .unwrap();
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    info!("start: {}, end: {}", start, end);
    tx.send(DatastoreOperation::FetchAmbientConditions {
        start: start.to_string(),
        end: end.to_string(),
        resp: resp_tx,
    })
    .await
    .unwrap();

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
