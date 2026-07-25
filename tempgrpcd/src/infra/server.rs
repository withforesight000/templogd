use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;
use tracing::{info, instrument};

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
    SD: FnOnce(Arc<Config>, tokio::sync::mpsc::Receiver<DatastoreOperation>, CancellationToken) -> JoinHandle<()>,
    SS: FnOnce(CancellationToken) -> SGFut,
    SGFut: std::future::Future<Output = ()> + Send,
    SG: FnOnce(
        tokio::sync::mpsc::Sender<DatastoreOperation>,
        Arc<Config>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>>,
{
    let cancellation_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let grpc_server = start_grpc(tx, config.clone()).await?;
    let redis_task = start_datastore(config.clone(), rx, cancellation_token.clone());
    start_signal_handler(cancellation_token.clone()).await;

    let addr = format!("{}:{}", config.get_server_bind_address(), config.get_server_port())
        .parse()
        .expect("Unable to parse socket address");

    let server_future = grpc_server.serve_with_shutdown(addr, cancellation_token.cancelled());

    info!("tempgrpcd listening on {}", addr);
    tokio::select! {
        _ = server_future => {
            info!("GRPC server was gracefully shut down");
        },
        _ = cancellation_token.cancelled() => {
            info!("cancellation token was cancelled");
        }
    }

    _ = tokio::join!(redis_task);
    Ok(())
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
                })
            };

        let start_signal_handler = |token: CancellationToken| async move {
            // Cancel shortly after the server starts.
            tokio::task::yield_now().await;
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
}
