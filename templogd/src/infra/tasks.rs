use std::{future::Future, pin::Pin, sync::Arc};

use common::gateway::datastore::DataStore;
use common::gateway::nature_remo_client::NatureRemoClient;
use common::model::repository::nature_remo::NatureRemo;
use scopeguard::defer;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::{config::Config, controller};
use common::infra::{async_redis_client::AsyncRedisCrateClient, http_client::ReqwestClient};
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>) {
    info!("Started");
    defer! {info!("Ended")}

    let cancellation_token = CancellationToken::new();
    let (tx, rx) = mpsc::channel(32);

    // A task that logs the temperature every 30 seconds to the console
    let nature_remo_client_factory = make_nature_remo_client_factory(config.clone());
    let datastore_factory = make_redis_client_factory(config.clone());

    let datastore_task = start_datastore_task(config.clone(), cancellation_token.clone(), rx, datastore_factory);
    let nature_remo_api_task = start_nature_remo_api_task(
        config.clone(),
        cancellation_token.clone(),
        tx,
        nature_remo_client_factory,
    );

    make_signal_handlers(cancellation_token.clone()).await;

    match tokio::try_join!(nature_remo_api_task, datastore_task) {
        Ok(_) => info!("All tasks completed successfully"),
        Err(e) => {
            error!("One of the tasks failed: {:?}", e);
            cancellation_token.cancel();
        }
    }
}

#[instrument(parent = None)]
fn make_nature_remo_client_factory(config: Arc<Config>) -> impl Fn() -> NatureRemoClient<ReqwestClient> {
    debug!("Started");
    defer! {debug!("Ended")}

    move || {
        NatureRemoClient::new(
            ReqwestClient::new(),
            config.get_api_token().to_string(),
            "https://api.nature.global".to_string(),
            config.get_device_id().to_string(),
        )
    }
}

#[instrument(parent = None)]
fn make_redis_client_factory(
    config: Arc<Config>,
) -> impl Fn() -> Pin<Box<dyn Future<Output = DataStore<AsyncRedisCrateClient>> + Send>> {
    debug!("Started");
    defer! {debug!("Ended")}

    fn redis_client(url: String) -> Pin<Box<dyn Future<Output = DataStore<AsyncRedisCrateClient>> + Send>> {
        Box::pin(async move { DataStore::new(AsyncRedisCrateClient::new(&url).await).await })
    }

    move || {
        redis_client(format!(
            "redis://{}:{}",
            config.get_redis_host(),
            config.get_redis_port()
        ))
    }
}

// TODO: remove skip(nature_remo_client)
#[instrument(parent = None, skip(nature_remo_client))]
fn start_nature_remo_api_task<R, T>(
    config: Arc<Config>,
    cancellation_token: CancellationToken,
    tx: mpsc::Sender<DatastoreOperation>,
    nature_remo_client: R,
) -> JoinHandle<()>
where
    R: FnOnce() -> T + Send + 'static,
    T: NatureRemo + Send + 'static, // 具体的な型を指定
{
    debug!("Started");
    defer! {debug!("Ended")}

    tokio::spawn(async move {
        let client = nature_remo_client();
        controller::log_temp::run(config, client, tx, cancellation_token).await;
    })
}

// TODO: remove skip(redis_client)
#[instrument(parent = None, skip(redis_client))]
fn start_datastore_task<F>(
    config: Arc<Config>,
    cancellation_token: CancellationToken,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    redis_client: F,
) -> JoinHandle<()>
where
    F: FnOnce() -> Pin<Box<dyn Future<Output = DataStore<AsyncRedisCrateClient>> + Send>> + Send + 'static,
{
    debug!("Started");
    defer! {debug!("Ended")}

    tokio::spawn(async move {
        let client = redis_client().await;
        controller::log_to_redis::run(config, client, rx, cancellation_token).await;
    })
}

#[instrument(parent = None)]
async fn make_signal_handlers(cancellation_token: CancellationToken) {
    debug!("Started");
    defer! {debug!("Ended")}

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("Failed to create SIGTERM signal listener");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received");
            cancellation_token.cancel();
        },
        _ = sigterm.recv() => {
            info!("SIGTERM received");
            cancellation_token.cancel();
        }
    }
}

// #codebase Can you write unit test code here?
// Please write tests that verify the behavior of private methods indirectly by testing the public methods that call
// them. Do not directly test or mock private methods. Instead, ensure that the public methods exercise all relevant
// branches and logic of the private methods during testing.
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use common::model::repository::ambient_condition::AmbientCondition;
//     use mockall::predicate::*;
//     use mockall::*;
//     use std::sync::Arc;
//     use std::error::Error;
//     use tokio::sync::mpsc;
//     use tokio::time::{sleep, Duration};
//     use tokio_util::sync::CancellationToken;

//     #[tokio::test]
//     async fn test_run() {
//         // Arrange
//         let config = Arc::new(Config::default());

//         common::infra::

//         // Create a cancellation token that we can control in the test
//         let cancellation_token = CancellationToken::new();

//         // We need to override the signal handling to trigger cancellation in the test
//         let (tx_signal, mut rx_signal) = mpsc::channel(1);

//         // Spawn the run function in a task
//         let run_handle = {
//             let config = config.clone();
//             let cancellation_token = cancellation_token.clone();

//             tokio::spawn(async move {
//                 // Simulate the signal handling
//                 tokio::select! {
//                     _ = rx_signal.recv() => {
//                         cancellation_token.cancel();
//                     }
//                 }

//                 // Call the actual run function
//                 super::run(config).await;
//             })
//         };

//         // Act
//         // Simulate sending a signal after some time
//         sleep(Duration::from_millis(100)).await;
//         tx_signal.send(()).await.unwrap();

//         // Wait for the run function to complete
//         let _ = run_handle.await;

//         // Assert
//         // If the run function completes without errors, the test passes
//         // You can add more assertions here to verify specific behaviors
//     }

//     #[tokio::test]
//     async fn test_start_nature_remo_api_task() {
//         // Arrange
//         let config = Arc::new(Config::default());
//         let cancellation_token = CancellationToken::new();
//         let (tx, mut rx) = mpsc::channel(32);

//         // Act
//         let handle = super::start_nature_remo_api_task(config.clone(), cancellation_token.clone(), tx);

//         // Cancel the token after some time to simulate shutdown
//         sleep(Duration::from_millis(100)).await;
//         cancellation_token.cancel();

//         // Wait for the task to complete
//         handle.await.unwrap();

//         // Assert
//         // Check that messages were sent over the channel
//         while let Some(_operation) = rx.recv().await {
//             // You can add assertions on the operations received
//         }
//     }

//     #[tokio::test]
//     async fn test_start_redis_task() {
//         // Arrange
//         let config = Arc::new(Config::default());
//         let cancellation_token = CancellationToken::new();
//         let (tx, rx) = mpsc::channel(32);

//         // Act
//         let handle = super::start_redis_task(config.clone(), cancellation_token.clone(), rx);

//         // Simulate sending some operations
//         tx.send(DatastoreOperation::default()).await.unwrap();

//         // Cancel the token after some time to simulate shutdown
//         sleep(Duration::from_millis(100)).await;
//         cancellation_token.cancel();

//         // Wait for the task to complete
//         handle.await.unwrap();

//         // Assert
//         // Since we don't have access to the internal state, we assume success if no errors occur
//     }
// }
