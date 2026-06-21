use common::model::ambient_condition::AmbientCondition;
use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use std::collections::HashMap;
use tracing::{debug, info, instrument};

use crate::usecase::error::UsecaseError;
use crate::usecase::port::GetAmbientConditions;

#[derive(Debug)]
pub struct GetAmbientConditionsUC {
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
}

impl GetAmbientConditionsUC {
    #[instrument(parent = None)]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Self {
        info!("Started");
        defer! {info!("Ended")}

        Self { tx }
    }
}

#[async_trait::async_trait]
impl GetAmbientConditions for GetAmbientConditionsUC {
    #[instrument(parent = None)]
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatastoreOperation::FetchAmbientConditions {
                start: start_time_seconds.to_string(),
                end: end_time_seconds.to_string(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition request channel closed"))?;
        info!("sent FetchAmbientConditions to fetch_from_redis task");

        let ambient_conditions = resp_rx
            .await
            .map_err(|_| UsecaseError::dependency_unavailable("ambient condition response channel closed"))??;

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
    async fn forwards_request_and_maps_response() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        // Spawn receiver to emulate fetch_from_redis
        let handle = tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { start, end, resp }) = rx.recv().await {
                assert_eq!(start, "0");
                assert_eq!(end, "1");
                let mut map = std::collections::HashMap::new();
                map.insert("k".into(), common::model::ambient_condition::new(1.0, 2.0, 3.0));
                resp.send(Ok(map)).unwrap();
            } else {
                panic!("no operation received");
            }
        });

        let resp = uc.run(0, 1).await.unwrap();
        assert_eq!(resp["k"].get_temperature(), 1.0);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn returns_channel_closed_if_request_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        drop(rx);

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition request channel closed")
        );
    }

    #[tokio::test]
    async fn returns_channel_closed_if_response_sender_is_closed() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                drop(resp);
            }
        });

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(
            matches!(err, UsecaseError::DependencyUnavailable(message) if message == "ambient condition response channel closed")
        );
    }

    #[tokio::test]
    async fn returns_storage_error_from_datastore() {
        let (tx, mut rx) = mpsc::channel(1);
        let uc = GetAmbientConditionsUC::new(tx);

        tokio::spawn(async move {
            if let Some(DatastoreOperation::FetchAmbientConditions { resp, .. }) = rx.recv().await {
                resp.send(Err(DataStoreError::Unavailable("redis unavailable".into()))).unwrap();
            }
        });

        let err = uc.run(0, 1).await.unwrap_err();

        assert!(matches!(
            err,
            UsecaseError::Storage(DataStoreError::Unavailable(message)) if message == "redis unavailable"
        ));
    }
}
