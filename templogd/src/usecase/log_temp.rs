use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use crate::config::Config;
use common::model::{channel::datastore_operation::DatastoreOperation, repository::nature_remo::NatureRemo};

#[instrument(name = "usecase.log_temp", skip_all)]
pub async fn run(
    _config: Arc<Config>,
    client: impl NatureRemo,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    loop {
        let nature_remo_result = client.fetch_ambient_condition().await;
        let condition = match nature_remo_result {
            Ok(condition) => Some(condition),
            Err(e) => {
                error!("Failed to fetch ambient condition from NatureRemo: {:?}", e);
                None
            }
        };

        if let Some(condition) = condition {
            info!(
                "Got ambient condition from NatureRemo: Temperature: {}, Humidity: {}, Illumination: {}",
                condition.get_temperature(),
                condition.get_humidity(),
                condition.get_illumination()
            );

            info!("Sending ambient condition to Redis task");
            let result = tx
                .send(DatastoreOperation::SaveAmbientCondition {
                    ambient_condition: condition,
                })
                .await;
            match result {
                Ok(_) => info!("Sent ambient condition to log_temp task"),
                Err(e) => error!("Failed to send ambient condition to log_temp task: {:?}", e),
            };
        }

        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {}
            _ = cancellation_token.cancelled() => {
                info!("confirmed cancellation token was cancelled");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::ambient_condition;
    use mockall::mock;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

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

    #[tokio::test]
    async fn sends_condition_on_success() {
        let mut client = MockNatureRemoClient::new();
        client.expect_fetch_ambient_condition().returning(|| Ok(ambient_condition::new(10.0, 20.0, 30.0)));

        let (tx, mut rx) = mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = run(config(), client, tx, token.clone());
        let receive_and_cancel = async {
            let received = rx.recv().await.unwrap();
            token.cancel();
            received
        };

        let (_, received) = tokio::join!(run_fut, receive_and_cancel);

        match received {
            DatastoreOperation::SaveAmbientCondition { ambient_condition } => {
                assert!((ambient_condition.get_temperature() - 10.0).abs() < f64::EPSILON)
            }
            _ => panic!("unexpected operation"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn does_not_send_when_fetch_fails() {
        let mut client = MockNatureRemoClient::new();
        client.expect_fetch_ambient_condition().returning(|| Err(Box::new(std::io::Error::other("fail"))));

        let (tx, mut rx) = mpsc::channel(1);
        let token = CancellationToken::new();

        let run_fut = run(config(), client, tx, token.clone());
        let cancel_soon = async {
            sleep(Duration::from_millis(20)).await;
            token.cancel();
        };

        let _ = tokio::join!(run_fut, cancel_soon);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_tx_failure_without_panicking() {
        let mut client = MockNatureRemoClient::new();
        client.expect_fetch_ambient_condition().returning(|| Ok(ambient_condition::new(10.0, 20.0, 30.0)));

        let (tx, rx) = mpsc::channel(1);
        drop(rx); // drop receiver to force send failure
        let token = CancellationToken::new();

        let run_fut = run(config(), client, tx, token.clone());
        let cancel_soon = async {
            sleep(Duration::from_millis(20)).await;
            token.cancel();
        };

        let _ = tokio::join!(run_fut, cancel_soon);
    }
}
