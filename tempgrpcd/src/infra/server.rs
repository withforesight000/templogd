use std::sync::Arc;

use askama::Template;
use scopeguard::defer;
use tempgrpcd_protos::tempgrpcd::v1::tempgrpcd_service_server::TempgrpcdServiceServer;
use tokio::{
    signal::{
        self,
        unix::{signal, SignalKind},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tonic::{
    metadata::MetadataValue,
    service::Interceptor,
    transport::{server::Router, Server},
    Request, Status,
};
use tonic_reflection::server::Builder;
use tracing::{debug, info, instrument};

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

#[derive(Clone)]
struct AuthInterceptor {
    token: String,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let correct_bearer_token = format!("Bearer {}", self.token);
        let token: MetadataValue<_> = correct_bearer_token.parse().unwrap();

        match req.metadata().get("authorization") {
            Some(t) if token == t => Ok(req),
            _ => Err(Status::unauthenticated("No valid auth token")),
        }
    }
}

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>) {
    info!("Started");
    defer! {info!("Ended")}

    let cancellation_token = CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let redis_task = start_datastore_task(config.clone(), rx, cancellation_token.clone());
    start_signal_handler_task(cancellation_token.clone()).await;

    let grpc_server = start_grpc_server_task(tx, config.clone());
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
fn start_datastore_task(
    config: Arc<Config>,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    debug!("Started");
    defer! {debug!("Ended")}

    tokio::spawn(async move {
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
fn start_grpc_server_task(tx: tokio::sync::mpsc::Sender<DatastoreOperation>, config: Arc<Config>) -> Router {
    debug!("Started");
    defer! {debug!("Ended")}

    let get_ambient_conditions_uc = GetAmbientConditionsUC::new(tx.clone());
    let get_ambient_conditions_with_sampling_uc = GetAmbientConditionsWithSamplingUC::new(tx);
    let ambient_condition_repository =
        GetAmbientConditionsImpl::new(get_ambient_conditions_uc, get_ambient_conditions_with_sampling_uc);

    Server::builder().add_service(TempgrpcdServiceServer::with_interceptor(
        ambient_condition_repository,
        AuthInterceptor {
            token: config.get_bearer_token().to_string(),
        },
    ))
    .add_service(
        Builder::configure()
            .register_encoded_file_descriptor_set(tempgrpcd_protos::tempgrpcd::v1::FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap(),
    )
}
