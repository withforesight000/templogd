use tokio::signal::{
    self,
    unix::{signal, SignalKind},
};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Status};
use tonic_reflection::server::Builder;
use tracing::{info, instrument};

use common::infra::null_nature_remo_client::NullNatureRemoClient;

use crate::{
    controller::{self, get_ambient_conditions},
    pb::tempgrpcd::tempgrpcd_server::TempgrpcdServer,
};

fn logging_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    info!("Received a request: {:?}", req);
    Ok(req)
}

#[instrument]
pub async fn run() {
    let cancellation_token = CancellationToken::new();

    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let addr = "0.0.0.0:50051".parse().unwrap();
    let ambient_condition_repository = get_ambient_conditions::GetAmbientConditions::new(tx);

    let cancellation_token_for_task_which_fetches_from_redis = cancellation_token.clone();
    let _task_which_fetches_from_redis = tokio::spawn(async move {
        let nature_remo_client = NullNatureRemoClient::new();
        let client =
            common::infra::async_redis_client::AsyncRedisCrateClient::new(&format!("redis://{}:{}", "redis", 6379))
                .await;
        let ambient_repository_repo =
            common::gateway::ambient_condition::AmbientConditionRepository::new(nature_remo_client, client);
        controller::fetch_from_redis::run(
            ambient_repository_repo,
            rx,
            cancellation_token_for_task_which_fetches_from_redis,
        )
        .await
    });

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create signal");
    let cancellation_token_for_signal_handler = cancellation_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("SIGINT received");

                cancellation_token_for_signal_handler.cancel();
                // break;
            },
            _ = sigterm.recv() => {
                info!("SIGTERM received");

                cancellation_token_for_signal_handler.cancel();
                // break;
            }
        }
    });

    let cancellation_token_for_grpc = cancellation_token.clone();
    let task_grpc_server = Server::builder()
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
        .serve_with_shutdown(addr, cancellation_token_for_grpc.cancelled());

    info!("tempgrpcd listening on {}", addr);
    tokio::select! {
        _ = task_grpc_server => {
            info!("GRPC server was gracefully shuted down");
        },
        _ = cancellation_token.cancelled() => {
            info!("cancellation token was cancelled");
        }
    }

    _ = tokio::join!(_task_which_fetches_from_redis);
}
