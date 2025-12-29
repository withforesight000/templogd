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

        let mut buffer = itoa::Buffer::new();
        let start_str = buffer.format(start.unwrap().seconds).to_owned();
        let end_str = buffer.format(end.unwrap().seconds).to_owned();
        let samples_str = buffer.format(samples).to_owned();

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start: start_str,
                end: end_str,
                samples: samples_str,
                resp: resp_tx,
            })
            .await
            .unwrap();
        info!("sent FetchAmbientConditions to fetch_from_redis task");

        let ambient_conditions = resp_rx.await.unwrap().unwrap();
        debug!("Received ambient conditions with sampling: {:?}", ambient_conditions);

        let mut proto_conditions = HashMap::with_capacity(ambient_conditions.len());
        for (key, value) in ambient_conditions {
            proto_conditions.insert(
                key,
                tempgrpcd_protos::tempgrpcd::v1::AmbientCondition {
                    temperature: value.get_temperature() as f32,
                    humidity: value.get_humidity() as f32,
                    illumination: value.get_illumination() as f32,
                },
            );
        }

        Ok(Response::new(GetAmbientConditionsResponse {
            ambient_conditions: proto_conditions,
        }))

        // Err(Status::unimplemented("Not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pbjson_types::Timestamp;
    use tempgrpcd_protos::tempgrpcd::v1::GetAmbientConditionsRequest;
    use tokio::sync::mpsc;
    use tonic::Request;

    #[tokio::test]
    async fn forwards_sampling_request() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsWithSamplingUC::new(tx);

        let req = Request::new(GetAmbientConditionsRequest {
            start_time: Some(Timestamp { seconds: 0, nanos: 0 }),
            end_time: Some(Timestamp { seconds: 1, nanos: 0 }),
            samples: Some(5),
        });

        let handle = tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start,
                end,
                samples,
                resp,
            }) = rx.recv().await
            {
                assert_eq!(start, "0");
                assert_eq!(end, "1");
                assert_eq!(samples, "5");
                let mut map = std::collections::HashMap::new();
                map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
                resp.send(Ok(map)).unwrap();
            } else {
                panic!("no operation received");
            }
        });

        let resp = uc.run(req).await.unwrap().into_inner();
        assert_eq!(resp.ambient_conditions["k"].humidity, 2.0);
        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn panics_if_response_missing() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsWithSamplingUC::new(tx);

        let req = Request::new(GetAmbientConditionsRequest {
            start_time: Some(Timestamp { seconds: 0, nanos: 0 }),
            end_time: Some(Timestamp { seconds: 1, nanos: 0 }),
            samples: Some(5),
        });

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditionsWithSampling { resp, .. }) = rx.recv().await {
                drop(resp);
            }
        });

        let _ = uc.run(req).await.unwrap();
    }
}
