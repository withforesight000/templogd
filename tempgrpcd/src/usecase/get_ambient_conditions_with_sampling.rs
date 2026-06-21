use common::model::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use std::collections::HashMap;
use tracing::{debug, info, instrument};

use crate::usecase::error::UsecaseError;
use crate::usecase::port::GetAmbientConditionsWithSampling;

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

#[async_trait::async_trait]
impl GetAmbientConditionsWithSampling for GetAmbientConditionsWithSamplingUC {
    #[instrument(parent = None)]
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
        samples: u64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditionsWithSampling {
                start: start_time_seconds.to_string(),
                end: end_time_seconds.to_string(),
                samples: samples.to_string(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition request channel closed"))?;
        info!("sent FetchAmbientConditions to fetch_from_redis task");

        let ambient_conditions = resp_rx
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition response channel closed"))??;
        debug!("Received ambient conditions with sampling: {:?}", ambient_conditions);

        Ok(ambient_conditions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usecase::error::UsecaseError;
    use common::model::repository::datastore::DataStoreError;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn forwards_sampling_request() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsWithSamplingUC::new(tx);

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

        let resp = uc.run(0, 1, 5).await.unwrap();
        assert_eq!(resp["k"].get_humidity(), 2.0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn returns_channel_closed_if_response_missing() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsWithSamplingUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditionsWithSampling { resp, .. }) = rx.recv().await {
                drop(resp);
            }
        });

        let err = uc.run(0, 1, 5).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition response channel closed")
        );
    }

    #[tokio::test]
    async fn returns_storage_error_from_datastore() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsWithSamplingUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditionsWithSampling { resp, .. }) = rx.recv().await {
                resp.send(Err(DataStoreError::Unavailable("redis unavailable".into()))).unwrap();
            }
        });

        let err = uc.run(0, 1, 5).await.unwrap_err();

        assert!(matches!(
            err,
            UsecaseError::Storage(DataStoreError::Unavailable(message)) if message == "redis unavailable"
        ));
    }
}
