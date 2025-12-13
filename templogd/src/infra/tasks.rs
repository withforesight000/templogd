use std::{future::Future, pin::Pin, sync::Arc};

use common::gateway::datastore::DataStore;
use common::gateway::nature_remo_client::NatureRemoClient;
use common::model::repository::datastore::DataStoreRepository;
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
    run_with(
        config,
        |token| make_signal_handlers(token),
        |cfg| make_nature_remo_client_factory(cfg),
        |cfg| make_redis_client_factory(cfg),
    )
    .await;
}

async fn run_with<S, SFut, NProvider, NFactory, NClient, DProvider, DFactory, DFut, DClient>(
    config: Arc<Config>,
    shutdown: S,
    nature_remo_client_factory: NProvider,
    datastore_factory: DProvider,
) where
    S: FnOnce(CancellationToken) -> SFut,
    SFut: Future<Output = ()> + Send,
    NProvider: FnOnce(Arc<Config>) -> NFactory,
    NFactory: FnOnce() -> NClient + Send + 'static,
    NClient: NatureRemo + Send + 'static,
    DProvider: FnOnce(Arc<Config>) -> DFactory,
    DFactory: FnOnce() -> DFut + Send + 'static,
    DFut: Future<Output = DClient> + Send + 'static,
    DClient: DataStoreRepository + Send + 'static,
{
    info!("Started");
    defer! {info!("Ended")}

    let cancellation_token = CancellationToken::new();
    let (tx, rx) = mpsc::channel(32);

    // A task that logs the temperature every 30 seconds to the console
    let nature_remo_client_factory = nature_remo_client_factory(config.clone());
    let datastore_factory = datastore_factory(config.clone());

    let datastore_task = start_datastore_task(config.clone(), cancellation_token.clone(), rx, datastore_factory);
    let nature_remo_api_task = start_nature_remo_api_task(
        config.clone(),
        cancellation_token.clone(),
        tx,
        nature_remo_client_factory,
    );

    shutdown(cancellation_token.clone()).await;

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
fn start_datastore_task<F, DFut, DClient>(
    config: Arc<Config>,
    cancellation_token: CancellationToken,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    redis_client: F,
) -> JoinHandle<()>
where
    F: FnOnce() -> DFut + Send + 'static,
    DFut: Future<Output = DClient> + Send + 'static,
    DClient: DataStoreRepository + Send + 'static,
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

#[cfg(test)]
mod tests {
    use super::*;
    use redis::ToRedisArgs;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    use std::time::Duration;

    #[derive(Debug)]
    struct StubNatureRemoOk;

    #[async_trait::async_trait]
    impl NatureRemo for StubNatureRemoOk {
        async fn fetch_ambient_condition(
            &self,
        ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error + Send>> {
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "no-op")))
        }
    }

    #[derive(Debug)]
    struct StubNatureRemoPanic;

    #[async_trait::async_trait]
    impl NatureRemo for StubNatureRemoPanic {
        async fn fetch_ambient_condition(
            &self,
        ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error + Send>> {
            panic!("boom")
        }
    }

    #[derive(Debug)]
    struct StubDatastore;

    #[async_trait::async_trait]
    impl DataStoreRepository for StubDatastore {
        async fn fetch_ambient_conditions<T: ToRedisArgs + Send + Sync + 'static + Debug>(
            &mut self,
            _start: T,
            _end: T,
        ) -> Result<HashMap<String, common::model::ambient_condition::AmbientCondition>, redis::RedisError> {
            Ok(HashMap::new())
        }

        async fn fetch_ambient_conditions_with_sampling<T: ToRedisArgs + Send + Sync + 'static + Debug>(
            &mut self,
            _start: T,
            _end: T,
            _samples: T,
        ) -> Result<HashMap<String, common::model::ambient_condition::AmbientCondition>, redis::RedisError> {
            Ok(HashMap::new())
        }

        async fn save_ambient_condition(
            &mut self,
            _ambient_condition: common::model::ambient_condition::AmbientCondition,
        ) -> Result<redis::Value, redis::RedisError> {
            Ok(redis::Value::Nil)
        }
    }

    fn config() -> Arc<Config> {
        crate::config::new(crate::TemplogdArgs {
            api_token: "token".to_string(),
            device_id: "device".to_string(),
            redis_host: "host".to_string(),
            redis_port: 6379,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_completes_on_shutdown() {
        let cfg = config();
        let shutdown = |token: CancellationToken| async move {
            token.cancel();
        };

        let nature_factory = |_cfg: Arc<Config>| move || StubNatureRemoOk;
        let datastore_factory = |_cfg: Arc<Config>| move || async move { StubDatastore };

        run_with(cfg, shutdown, nature_factory, datastore_factory).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_handles_task_failure_and_cancels() {
        let cfg = config();
        let shutdown = |token: CancellationToken| {
            async move {
                // Let the spawned task run first.
                tokio::task::yield_now().await;
                token.cancel();
            }
        };

        let nature_factory = |_cfg: Arc<Config>| move || StubNatureRemoPanic;
        let datastore_factory = |_cfg: Arc<Config>| move || async move { StubDatastore };

        // Should not panic even if one of the tasks panics.
        run_with(cfg, shutdown, nature_factory, datastore_factory).await;
    }

    #[test]
    fn make_nature_remo_client_factory_masks_token() {
        let cfg = crate::config::new(crate::TemplogdArgs {
            api_token: "super-secret".into(),
            device_id: "device".into(),
            redis_host: "host".into(),
            redis_port: 6379,
        });
        let factory = make_nature_remo_client_factory(cfg);
        let client = factory();

        let debug = format!("{:?}", client);
        assert!(debug.contains("api.nature.global"));
        assert!(debug.contains("<MASKED>"));
        assert!(!debug.contains("super-secret"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_nature_remo_api_task_sends_and_stops_on_cancel() {
        #[derive(Debug)]
        struct StubNatureRemoOnce {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl NatureRemo for StubNatureRemoOnce {
            async fn fetch_ambient_condition(
                &self,
            ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error + Send>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(common::model::ambient_condition::new(1.0, 2.0, 3.0))
            }
        }

        let cfg = config();
        let token = CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let calls = Arc::new(AtomicUsize::new(0));

        let factory_calls = calls.clone();
        let handle = start_nature_remo_api_task(cfg, token.clone(), tx, move || StubNatureRemoOnce { calls: factory_calls });

        let op = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.unwrap().unwrap();
        token.cancel();
        handle.await.unwrap();

        match op {
            DatastoreOperation::SaveAmbientCondition { .. } => {}
            _ => panic!("unexpected operation"),
        }
        assert!(calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_datastore_task_saves_and_stops() {
        #[derive(Debug)]
        struct StubDatastoreSave {
            saved: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl DataStoreRepository for StubDatastoreSave {
            async fn fetch_ambient_conditions<T: ToRedisArgs + Send + Sync + 'static + Debug>(
                &mut self,
                _start: T,
                _end: T,
            ) -> Result<HashMap<String, common::model::ambient_condition::AmbientCondition>, redis::RedisError> {
                Ok(HashMap::new())
            }

            async fn fetch_ambient_conditions_with_sampling<T: ToRedisArgs + Send + Sync + 'static + Debug>(
                &mut self,
                _start: T,
                _end: T,
                _samples: T,
            ) -> Result<HashMap<String, common::model::ambient_condition::AmbientCondition>, redis::RedisError> {
                Ok(HashMap::new())
            }

            async fn save_ambient_condition(
                &mut self,
                _ambient_condition: common::model::ambient_condition::AmbientCondition,
            ) -> Result<redis::Value, redis::RedisError> {
                self.saved.fetch_add(1, Ordering::SeqCst);
                Ok(redis::Value::Nil)
            }
        }

        let cfg = config();
        let token = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let saved = Arc::new(AtomicUsize::new(0));
        let saved_clone = saved.clone();

        let handle = start_datastore_task(cfg, token.clone(), rx, move || async move {
            StubDatastoreSave { saved: saved_clone }
        });

        tx.send(DatastoreOperation::SaveAmbientCondition {
            ambient_condition: common::model::ambient_condition::new(1.0, 2.0, 3.0),
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_millis(200), async {
            while saved.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        token.cancel();
        handle.await.unwrap();
        assert_eq!(saved.load(Ordering::SeqCst), 1);
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
