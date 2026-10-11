use crate::usecase;
use common::model::{channel::datastore_operation::DatastoreOperation, repository::datastore::DataStoreRepository};

use tokio_util::sync::CancellationToken;
use tracing::instrument;

#[instrument(level = "info", name = "controller.fetch_from_redis", skip_all)]
pub async fn run(
    client: impl DataStoreRepository + Send,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::fetch_from_redis::run(client, rx, cancellation_token).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use common::model::repository::datastore::DataStoreError;
    use mockall::mock;
    use redis::{RedisError, ToRedisArgs, Value};
    use tokio::sync::oneshot;

    mock! {
        pub DataStore {}

        #[async_trait::async_trait]
        impl DataStoreRepository for DataStore {
            async fn fetch_ambient_conditions<
                T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
                max_entries: usize,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, DataStoreError>;

            async fn fetch_ambient_conditions_with_sampling<
                T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
                samples: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, DataStoreError>;

            async fn save_ambient_condition(
                &mut self,
                ambient_condition: ambient_condition::AmbientCondition,
            ) -> Result<Value, RedisError>;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn controller_delegates_fetch_operation() {
        let mut datastore = MockDataStore::new();
        datastore.expect_fetch_ambient_conditions().returning(|start: String, end: String, max_entries| {
            assert_eq!(start, "0");
            assert_eq!(end, "1");
            assert_eq!(max_entries, 5000);
            Ok(std::collections::HashMap::new())
        });

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = super::run(datastore, rx, token.clone());
        let send_and_cancel = async {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.send(DatastoreOperation::FetchAmbientConditions {
                start: "0".into(),
                end: "1".into(),
                max_entries: 5000,
                deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(1),
                cancellation: CancellationToken::new(),
                span: tracing::Span::current(),
                resp: resp_tx,
            })
            .await
            .unwrap();

            assert!(resp_rx.await.unwrap().is_ok());
            token.cancel();
        };

        let _ = tokio::join!(run_fut, send_and_cancel);
    }
}
