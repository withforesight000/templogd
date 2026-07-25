use std::sync::Arc;

use askama::Template;
use tempgrpcd_protos::tempgrpcd::v1::tempgrpcd_service_server::TempgrpcdServiceServer;
use tokio::{
    signal::{
        self,
        unix::{SignalKind, signal},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::Interceptor,
    transport::{Server, server::Router},
};
use tonic_reflection::server::Builder;
use tracing::{Instrument, info, info_span, instrument, warn};

use crate::{
    config::Config,
    controller::{self, get_ambient_conditions::GetAmbientConditionsImpl},
    usecase::{
        get_ambient_conditions::GetAmbientConditionsUC,
        get_ambient_conditions_with_sampling::GetAmbientConditionsWithSamplingUC,
    },
};
use common::{gateway::datastore::DataStore, model::channel::datastore_operation::DatastoreOperation};

pub const REDIS_XRANGE_WITH_SAMPLING: &str = "xrange_with_sampling";
type TempgrpcdController = GetAmbientConditionsImpl<GetAmbientConditionsUC, GetAmbientConditionsWithSamplingUC>;

#[derive(Clone)]
struct AuthInterceptor {
    token: String,
}

impl Interceptor for AuthInterceptor {
    #[instrument(level = "info", name = "grpc.authenticate", skip_all)]
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let correct_bearer_token = format!("Bearer {}", self.token);
        let token: MetadataValue<_> = correct_bearer_token.parse().unwrap();

        match req.metadata().get("authorization") {
            Some(t) if token == t => Ok(req),
            _ => {
                warn!("gRPC authentication failed");
                Err(Status::unauthenticated("No valid auth token"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_allows_valid_token() {
        let mut interceptor = AuthInterceptor { token: "secret".into() };
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer secret").unwrap());
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn interceptor_rejects_invalid_token() {
        let mut interceptor = AuthInterceptor { token: "secret".into() };
        let req = Request::new(());
        let err = interceptor.call(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}

/// Starts the tempgrpcd server, Redis worker, and shutdown handling.
#[instrument(level = "info", parent = None)]
pub async fn run(config: Arc<Config>) {
    run_with(
        config,
        start_datastore_task,
        start_signal_handler_task,
        boxed_start_grpc_server_task,
    )
    .await;
}

#[instrument(level = "debug", skip_all)]
async fn run_with<SD, SS, SG, SGFut>(config: Arc<Config>, start_datastore: SD, start_signal_handler: SS, start_grpc: SG)
where
    SD: FnOnce(Arc<Config>, tokio::sync::mpsc::Receiver<DatastoreOperation>, CancellationToken) -> JoinHandle<()>,
    SS: FnOnce(CancellationToken) -> SGFut,
    SGFut: std::future::Future<Output = ()> + Send,
    SG: FnOnce(
        tokio::sync::mpsc::Sender<DatastoreOperation>,
        Arc<Config>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Router> + Send>>,
{
    let cancellation_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let redis_task = start_datastore(config.clone(), rx, cancellation_token.clone());
    start_signal_handler(cancellation_token.clone()).await;

    let grpc_server = start_grpc(tx, config.clone()).await;
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
}

#[instrument(level = "debug", skip_all)]
fn boxed_start_grpc_server_task(
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    config: Arc<Config>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Router> + Send>> {
    Box::pin(start_grpc_server_task(tx, config))
}

#[instrument(level = "debug", skip_all)]
fn start_datastore_task(
    config: Arc<Config>,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    let task_span = info_span!("tempgrpcd.redis_task");
    tokio::spawn(
        async move {
            #[derive(Template)]
            #[template(path = "xrange_with_sampling.lua.j2")]
            struct XRANGEWithSamplingTemplate<'a> {
                function_name: &'a str,
            }
            let xrange_with_sampling_code = XRANGEWithSamplingTemplate {
                function_name: REDIS_XRANGE_WITH_SAMPLING,
            }
            .render()
            .expect("Failed to render template");

            let mut datastore_client = DataStore::new(
                common::infra::async_redis_client::AsyncRedisCrateClient::new(&format!(
                    "redis://{}:{}",
                    config.get_redis_host(),
                    config.get_redis_port()
                ))
                .await,
            )
            .await;
            datastore_client
                .load_function_xrange_with_sampling(&xrange_with_sampling_code)
                .await
                .expect("Failed to load Lua script for xrange with sampling");
            controller::fetch_from_redis::run(datastore_client, rx, cancellation_token).await
        }
        .instrument(task_span),
    )
}

#[instrument(level = "debug", skip_all)]
async fn start_signal_handler_task(cancellation_token: CancellationToken) {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create signal");
    tokio::spawn(
        async move {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("SIGINT received");

                    cancellation_token.cancel();
                    // break;
                },
                _ = sigterm.recv() => {
                    info!("SIGTERM received");

                    cancellation_token.cancel();
                    // break;
                }
            }
        }
        .instrument(info_span!("tempgrpcd.signal_handler_task")),
    );
}

#[instrument(level = "debug", parent = None, skip_all)]
async fn start_grpc_server_task(tx: tokio::sync::mpsc::Sender<DatastoreOperation>, config: Arc<Config>) -> Router {
    let get_ambient_conditions_uc = GetAmbientConditionsUC::new(tx.clone());
    let get_ambient_conditions_with_sampling_uc = GetAmbientConditionsWithSamplingUC::new(tx);
    let grpc_service = TempgrpcdController::new(get_ambient_conditions_uc, get_ambient_conditions_with_sampling_uc);
    let (reporter, health_server) = tonic_health::server::health_reporter();
    reporter.set_serving::<TempgrpcdServiceServer<TempgrpcdController>>().await;

    Server::builder()
        .trace_fn(|request| {
            let trace_id = super::request_tracing::new_trace_id();
            info_span!(
                "grpc.request",
                trace_id = %trace_id,
                method = %request.uri().path(),
            )
        })
        .add_service(TempgrpcdServiceServer::with_interceptor(
            grpc_service,
            AuthInterceptor {
                token: config.get_bearer_token().to_string(),
            },
        ))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(tempgrpcd_protos::tempgrpcd::v1::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1()
                .unwrap(),
        )
        .add_service(health_server)
}

#[cfg(test)]
mod run_tests {
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
        let _router = start_grpc_server_task(tx, config).await;
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
                as std::pin::Pin<Box<dyn std::future::Future<Output = Router> + Send>>
        };

        let fut = run_with(config, start_datastore, start_signal_handler, start_grpc);
        tokio::time::timeout(std::time::Duration::from_secs(2), fut).await.expect("server did not shut down in time");
    }
}
