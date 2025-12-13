use std::sync::Arc;

use common::model::repository::nature_remo::NatureRemo;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl NatureRemo,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    usecase::log_temp::run(config, client, tx, cancellation_token).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;

    mock! {
        pub NatureRemoClient {}

        #[async_trait::async_trait]
        impl NatureRemo for NatureRemoClient {
            async fn fetch_ambient_condition(
                &self,
            ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error + Send>>;
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
    async fn controller_delegates_to_usecase_and_sends_operation() {
        let mut client = MockNatureRemoClient::new();
        client
            .expect_fetch_ambient_condition()
            .returning(|| Ok(ambient_condition::new(1.0, 2.0, 3.0)));

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = super::run(config(), client, tx, token.clone());
        let recv_and_cancel = async {
            let op = rx.recv().await.unwrap();
            token.cancel();
            op
        };

        let (_, op) = tokio::join!(run_fut, recv_and_cancel);
        match op {
            DatastoreOperation::SaveAmbientCondition { .. } => {}
            _ => panic!("unexpected operation"),
        }
    }
}
