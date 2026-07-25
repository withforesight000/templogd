use std::sync::Arc;

use tempgrpcd_protos::tempgrpcd::v1::tempgrpcd_service_server::TempgrpcdServiceServer;
use tokio::sync::mpsc;
use tonic::transport::{Server, server::Router};
use tonic_reflection::server::Builder;
use tracing::{info_span, instrument};

use super::{
    auth::{AuthInterceptor, ServerError},
    request_tracing,
};
use crate::{
    config::Config,
    controller::get_ambient_conditions::GetAmbientConditionsImpl,
    usecase::{
        get_ambient_conditions::GetAmbientConditionsUC,
        get_ambient_conditions_with_sampling::GetAmbientConditionsWithSamplingUC,
    },
};
use common::model::channel::datastore_operation::DatastoreOperation;

type TempgrpcdController = GetAmbientConditionsImpl<GetAmbientConditionsUC, GetAmbientConditionsWithSamplingUC>;

/// Boxes the gRPC router future so the server startup pipeline can inject it in tests.
#[instrument(level = "debug", name = "infra.boxed_start_grpc_server_task", skip_all)]
pub(super) fn boxed_start_grpc_server_task(
    tx: mpsc::Sender<DatastoreOperation>,
    config: Arc<Config>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Router, ServerError>> + Send>> {
    // Tokio repeatedly calls a Future's `poll` method to drive its execution and
    // ask whether it can make progress: it returns `Pending` when it must wait
    // for I/O and `Ready` when it has finished. An `async fn` Future stores its
    // state across `await` points and may contain references to that state, so it
    // must remain at a stable memory address while Tokio polls it. `Pin`
    // provides that guarantee, and `Box::pin` stores the Future in a stable
    // heap allocation.
    Box::pin(start_grpc_server_task(tx, config))
}

/// Builds the gRPC router and validates authentication configuration before serving requests.
#[instrument(level = "debug", name = "infra.start_grpc_server_task", parent = None, skip_all)]
pub(super) async fn start_grpc_server_task(
    tx: mpsc::Sender<DatastoreOperation>,
    config: Arc<Config>,
) -> Result<Router, ServerError> {
    let auth_interceptor = AuthInterceptor::new(config.get_bearer_token()).map_err(ServerError::InvalidBearerToken)?;
    let get_ambient_conditions_uc = GetAmbientConditionsUC::new(tx.clone());
    let get_ambient_conditions_with_sampling_uc = GetAmbientConditionsWithSamplingUC::new(tx);
    let grpc_service = TempgrpcdController::new(get_ambient_conditions_uc, get_ambient_conditions_with_sampling_uc);
    let (reporter, health_server) = tonic_health::server::health_reporter();
    reporter.set_serving::<TempgrpcdServiceServer<TempgrpcdController>>().await;

    Ok(Server::builder()
        .trace_fn(|request| {
            let trace_id = request_tracing::new_trace_id();
            info_span!(
                "infra.grpc.request",
                trace_id = %trace_id,
                method = %request.uri().path(),
            )
        })
        .add_service(TempgrpcdServiceServer::with_interceptor(grpc_service, auth_interceptor))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(tempgrpcd_protos::tempgrpcd::v1::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1()
                .unwrap(),
        )
        .add_service(health_server))
}
