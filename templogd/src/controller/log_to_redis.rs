use std::sync::Arc;

use common::model::repository::datastore::DataStoreRepository;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(name = "controller.log_to_redis", skip_all)]
pub async fn run(
    config: Arc<Config>,
    client: impl DataStoreRepository,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    usecase::log_to_redis::run(config, client, rx, cancellation_token).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;
    use redis::{RedisError, ToRedisArgs, Value};

    mock! {
        pub DataStore {}

        #[async_trait::async_trait]
        impl DataStoreRepository for DataStore {
            async fn fetch_ambient_conditions<
                T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, RedisError>;

            async fn fetch_ambient_conditions_with_sampling<
                T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static + std::fmt::Debug,
            >(
                &mut self,
                start: T,
                end: T,
                samples: T,
            ) -> Result<std::collections::HashMap<String, ambient_condition::AmbientCondition>, RedisError>;

            async fn save_ambient_condition(
                &mut self,
                ambient_condition: ambient_condition::AmbientCondition,
            ) -> Result<Value, RedisError>;
        }
    }

    fn config() -> Arc<Config> {
        crate::config::new(crate::TemplogdArgs {
            api_token: "".to_string(),
            device_id: "".to_string(),
            redis_host: "".to_string(),
            redis_port: 0,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn controller_delegates_to_usecase_and_saves() {
        let mut datastore = MockDataStore::new();
        datastore.expect_save_ambient_condition().returning(|_| Ok(Value::Nil));

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = super::run(config(), datastore, rx, token.clone());
        let send_and_cancel = async {
            tx.send(DatastoreOperation::SaveAmbientCondition {
                ambient_condition: ambient_condition::new(1.0, 2.0, 3.0),
            })
            .await
            .unwrap();
            token.cancel();
        };

        let _ = tokio::join!(run_fut, send_and_cancel);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panics_on_fetch_operation() {
        let datastore = MockDataStore::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();

        let handle = tokio::spawn(run(config(), datastore, rx, token));

        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditions {
            start: "0".into(),
            end: "1".into(),
            resp: resp_tx,
        })
        .await
        .unwrap();

        let join = handle.await;
        assert!(join.is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panics_on_sampling_operation() {
        let datastore = MockDataStore::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();

        let handle = tokio::spawn(run(config(), datastore, rx, token));

        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        tx.send(DatastoreOperation::FetchAmbientConditionsWithSampling {
            start: "0".into(),
            end: "1".into(),
            samples: "2".into(),
            resp: resp_tx,
        })
        .await
        .unwrap();

        let join = handle.await;
        assert!(join.is_err());
    }
}
