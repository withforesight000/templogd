use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;
use tracing::{error, info, instrument};

use super::{
    auth::ServerError,
    grpc::boxed_start_grpc_server_task,
    tasks::{start_datastore_task, start_signal_handler_task},
};
use crate::config::Config;
use common::model::channel::datastore_operation::DatastoreOperation;

/// Starts the tempgrpcd server, Redis worker, and shutdown handling.
///
/// Returns an error when the configured bearer token is not valid gRPC metadata.
#[instrument(level = "info", name = "infra.run", parent = None)]
pub(crate) async fn run(config: Arc<Config>) -> Result<(), ServerError> {
    run_with(
        config,
        start_datastore_task,
        start_signal_handler_task,
        boxed_start_grpc_server_task,
    )
    .await
}

#[instrument(level = "debug", name = "infra.run_with", skip_all)]
async fn run_with<SD, SS, SG, SGFut>(
    config: Arc<Config>,
    start_datastore: SD,
    start_signal_handler: SS,
    start_grpc: SG,
) -> Result<(), ServerError>
where
    SD: FnOnce(
        Arc<Config>,
        tokio::sync::mpsc::Receiver<DatastoreOperation>,
        CancellationToken,
    ) -> JoinHandle<Result<(), ServerError>>,
    SS: FnOnce(CancellationToken) -> SGFut,
    SGFut: std::future::Future<Output = ()> + Send,
    SG: FnOnce(
        tokio::sync::mpsc::Sender<DatastoreOperation>,
        Arc<Config>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>,
{
    let addr = format!("{}:{}", config.get_server_bind_address(), config.get_server_port())
        .parse()
        .map_err(ServerError::InvalidBindAddress)?;
    let cancellation_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let grpc_server = start_grpc(tx, config.clone()).await?;
    let mut redis_task = start_datastore(config.clone(), rx, cancellation_token.clone());
    start_signal_handler(cancellation_token.clone()).await;

    let server_future = grpc_server.serve_with_shutdown(addr, cancellation_token.cancelled());
    tokio::pin!(server_future);

    info!("Starting tempgrpcd listener on {}", addr);
    enum FirstCompletion {
        Shutdown,
        GrpcServer(Result<(), tonic::transport::Error>),
        Redis(Result<Result<(), ServerError>, tokio::task::JoinError>),
    }

    let first_completion = tokio::select! {
        biased;
        _ = cancellation_token.cancelled() => {
            info!(reason = "shutdown_requested", "Shutdown requested");
            FirstCompletion::Shutdown
        },
        result = &mut redis_task => {
            FirstCompletion::Redis(result)
        },
        result = &mut server_future => {
            FirstCompletion::GrpcServer(result)
        }
    };

    cancellation_token.cancel();
    match first_completion {
        FirstCompletion::Shutdown => {
            let (server_result, redis_result) = tokio::join!(server_future, redis_task);
            server_result.map_err(ServerError::ServeGrpc)?;
            handle_shutdown_task_result("redis", redis_result)
        }
        FirstCompletion::GrpcServer(server_result) => {
            log_shutdown_task_result("redis", redis_task.await);
            server_result.map_err(ServerError::ServeGrpc)?;
            Err(ServerError::BackgroundTaskStopped { task: "grpc" })
        }
        FirstCompletion::Redis(redis_result) => {
            if let Err(error) = server_future.await {
                error!(error = %error, "gRPC server failed during Redis worker shutdown");
            }
            handle_active_task_result("redis", redis_result)
        }
    }
}

fn handle_active_task_result(
    task: &'static str,
    result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    match result {
        Ok(Ok(())) => Err(ServerError::BackgroundTaskStopped { task }),
        Ok(Err(error)) => Err(error),
        Err(source) => Err(ServerError::BackgroundTask { task, source }),
    }
}

fn handle_shutdown_task_result(
    task: &'static str,
    result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(source) => Err(ServerError::BackgroundTask { task, source }),
    }
}

fn log_shutdown_task_result(task: &'static str, result: Result<Result<(), ServerError>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => info!(task, "Background task completed during shutdown"),
        Ok(Err(error)) => error!(task, error = %error, "Background task failed during shutdown"),
        Err(error) => error!(task, error = %error, "Background task panicked during shutdown"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::grpc::start_grpc_server_task;
    use super::*;

    fn args() -> crate::TempgrpcdArgs {
        crate::TempgrpcdArgs {
            server_bind_address: "127.0.0.1".into(),
            server_port: "0".into(),
            bearer_token: "token".into(),
            redis_host: "localhost".into(),
            redis_port: 6379,
            log_format: crate::LogFormat::Json,
            log_level: crate::LogLevel::Info,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn builds_grpc_router() {
        let config = crate::config::new(args());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let _router = start_grpc_server_task(tx, config).await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_invalid_bearer_token_before_building_router() {
        let mut invalid_args = args();
        invalid_args.bearer_token = "token\n".into();
        let config = crate::config::new(invalid_args);
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let result = start_grpc_server_task(tx, config).await;

        assert!(matches!(result, Err(ServerError::InvalidBearerToken(_))));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_shuts_down_without_redis() {
        let config = crate::config::new(args());

        let start_datastore =
            |_cfg: Arc<Config>, _rx: tokio::sync::mpsc::Receiver<DatastoreOperation>, token: CancellationToken| {
                tokio::spawn(async move {
                    token.cancelled().await;
                    Ok(())
                })
            };

        let start_signal_handler = |token: CancellationToken| async move {
            token.cancel();
        };

        let start_grpc = |tx: tokio::sync::mpsc::Sender<DatastoreOperation>, cfg: Arc<Config>| {
            Box::pin(start_grpc_server_task(tx, cfg))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>
        };

        let fut = run_with(config, start_datastore, start_signal_handler, start_grpc);
        tokio::time::timeout(std::time::Duration::from_secs(2), fut)
            .await
            .expect("server did not shut down in time")
            .expect("server failed to start");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_returns_an_error_when_redis_worker_exits() {
        let config = crate::config::new(args());

        let start_datastore =
            |_cfg: Arc<Config>, _rx: tokio::sync::mpsc::Receiver<DatastoreOperation>, _token: CancellationToken| {
                tokio::spawn(async { Ok(()) })
            };

        let start_signal_handler = |_token: CancellationToken| async {};
        let start_grpc = |tx: tokio::sync::mpsc::Sender<DatastoreOperation>, cfg: Arc<Config>| {
            Box::pin(start_grpc_server_task(tx, cfg))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>
        };

        let fut = run_with(config, start_datastore, start_signal_handler, start_grpc);
        let error = tokio::time::timeout(std::time::Duration::from_secs(2), fut)
            .await
            .expect("server did not stop after the Redis worker exited")
            .unwrap_err();

        assert!(matches!(error, ServerError::BackgroundTaskStopped { task: "redis" }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_returns_redis_worker_panics() {
        let config = crate::config::new(args());

        let start_datastore =
            |_cfg: Arc<Config>, _rx: tokio::sync::mpsc::Receiver<DatastoreOperation>, _token: CancellationToken| {
                tokio::spawn(async {
                    panic!("redis worker panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            };
        let start_signal_handler = |_token: CancellationToken| async {};
        let start_grpc = |tx: tokio::sync::mpsc::Sender<DatastoreOperation>, cfg: Arc<Config>| {
            Box::pin(start_grpc_server_task(tx, cfg))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_with(config, start_datastore, start_signal_handler, start_grpc),
        )
        .await
        .expect("server did not stop after the Redis worker panicked")
        .unwrap_err();

        assert!(matches!(error, ServerError::BackgroundTask { task: "redis", .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_returns_grpc_bind_failures() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut server_args = args();
        server_args.server_port = port.to_string();
        let config = crate::config::new(server_args);

        let start_datastore =
            |_cfg: Arc<Config>, _rx: tokio::sync::mpsc::Receiver<DatastoreOperation>, token: CancellationToken| {
                tokio::spawn(async move {
                    token.cancelled().await;
                    Ok(())
                })
            };
        let start_signal_handler = |_token: CancellationToken| async {};
        let start_grpc = |tx: tokio::sync::mpsc::Sender<DatastoreOperation>, cfg: Arc<Config>| {
            Box::pin(start_grpc_server_task(tx, cfg))
                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_with(config, start_datastore, start_signal_handler, start_grpc),
        )
        .await
        .expect("server did not return its bind failure")
        .unwrap_err();

        assert!(matches!(error, ServerError::ServeGrpc(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_with_rejects_invalid_bind_addresses_before_starting_tasks() {
        let mut server_args = args();
        server_args.server_port = "invalid".into();
        let config = crate::config::new(server_args);

        let start_datastore =
            |_cfg: Arc<Config>, _rx: tokio::sync::mpsc::Receiver<DatastoreOperation>, _token: CancellationToken| {
                panic!("datastore task must not start");
            };
        let start_signal_handler = |_token: CancellationToken| async {
            panic!("signal handler must not start");
        };
        let start_grpc = |_tx: tokio::sync::mpsc::Sender<DatastoreOperation>, _cfg: Arc<Config>| {
            panic!("gRPC router must not start");
        };

        let error = run_with(config, start_datastore, start_signal_handler, start_grpc).await.unwrap_err();

        assert!(matches!(error, ServerError::InvalidBindAddress(_)));
    }
}
