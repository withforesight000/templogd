use std::sync::Arc;

use scopeguard::defer;
use tokio::{
    signal::{
        self,
        unix::{signal, SignalKind},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tonic::{
    transport::{server::Router, Server},
    Request, Status,
};
use tonic_reflection::server::Builder;
use tracing::{debug, info, instrument};

use crate::{
    config::Config,
    controller::{self, get_ambient_conditions},
    pb::tempgrpcd::tempgrpcd_server::TempgrpcdServer,
};
use common::model::channel::datastore_operation::DatastoreOperation;

fn logging_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    info!("Received a request: {:?}", req);
    Ok(req)
}

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>) {
    info!("Started");
    defer! {info!("Ended")}

    let cancellation_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let redis_task = start_redis_task(config.clone(), rx, cancellation_token.clone());
    start_signal_handler_task(cancellation_token.clone()).await;

    let grpc_server = start_grpc_server_task(tx);
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

#[instrument(parent = None)]
fn start_redis_task(
    config: Arc<Config>,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    debug!("Started");
    defer! {debug!("Ended")}

    tokio::spawn(async move {
        let redis_client = common::infra::async_redis_client::AsyncRedisCrateClient::new(&format!(
            "redis://{}:{}",
            config.get_redis_host(),
            config.get_redis_port()
        ))
        .await;
        controller::fetch_from_redis::run(redis_client, rx, cancellation_token).await
    })
}

#[instrument(parent = None)]
async fn start_signal_handler_task(cancellation_token: CancellationToken) {
    debug!("Started");
    defer! {debug!("Ended")}

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create signal");
    tokio::spawn(async move {
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
    });
}

#[instrument(parent = None)]
fn start_grpc_server_task(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Router {
    debug!("Started");
    defer! {debug!("Ended")}

    let ambient_condition_repository = get_ambient_conditions::GetAmbientConditions::new(tx);

    Server::builder()
        .add_service(TempgrpcdServer::with_interceptor(
            ambient_condition_repository,
            logging_interceptor,
        ))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(tonic::include_file_descriptor_set!("tempgrpcd"))
                .build_v1()
                .unwrap(),
        )
}
