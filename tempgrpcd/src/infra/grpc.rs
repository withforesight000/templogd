use std::sync::Arc;

use tempgrpcd_protos::tempgrpcd::v1::tempgrpcd_service_server::TempgrpcdServiceServer;
use tokio::sync::mpsc;
use tonic::{
    service::interceptor::InterceptedService,
    transport::{Server, server::Router},
};
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
    let auth_interceptor = AuthInterceptor::new(
        config.get_bearer_token(),
        config.get_trusted_proxy_ips(),
        config.allow_direct_clients(),
    )
    .map_err(ServerError::InvalidBearerToken)?;
    let get_ambient_conditions_uc = GetAmbientConditionsUC::new(tx.clone(), config.get_max_result_entries());
    let get_ambient_conditions_with_sampling_uc = GetAmbientConditionsWithSamplingUC::new(tx);
    let grpc_service = TempgrpcdController::new(
        get_ambient_conditions_uc,
        get_ambient_conditions_with_sampling_uc,
        config.get_max_samples(),
        config.get_max_concurrent_requests(),
        config.get_request_timeout(),
        config.accept_legacy_millisecond_timestamps(),
    );
    let (reporter, health_server) = tonic_health::server::health_reporter();
    reporter.set_serving::<TempgrpcdServiceServer<TempgrpcdController>>().await;

    let reflection = Builder::configure()
        .register_encoded_file_descriptor_set(tempgrpcd_protos::tempgrpcd::v1::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(ServerError::BuildReflection)?;

    Ok(Server::builder()
        .trace_fn(|request| {
            let trace_id = request_tracing::new_trace_id();
            info_span!(
                "infra.grpc.request",
                trace_id = %trace_id,
                method = %request.uri().path(),
            )
        })
        .add_service(TempgrpcdServiceServer::with_interceptor(
            grpc_service,
            auth_interceptor.clone(),
        ))
        .add_service(reflection)
        .add_service(InterceptedService::new(health_server, auth_interceptor)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;
    use tonic::{
        Code, Request,
        transport::{Channel, server::TcpIncoming},
    };
    use tonic_health::pb::{HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient};

    /// Starts the production router on an ephemeral port without a Redis worker.
    async fn server(
        direct: bool,
        trusted: &str,
    ) -> (HealthClient<Channel>, CancellationToken, tokio::task::JoinHandle<()>) {
        let config = crate::config::new(crate::TempgrpcdArgs {
            server_bind_address: "127.0.0.1".into(),
            server_port: "0".into(),
            bearer_token: "test-token".into(),
            redis_host: "localhost".into(),
            redis_port: 6379,
            trusted_proxy_ips: trusted.into(),
            allow_direct_clients: direct,
            max_result_entries: 5000,
            max_samples: 1000,
            max_concurrent_requests: 4,
            request_timeout_milliseconds: 5000,
            accept_legacy_millisecond_timestamps: false,
            log_format: crate::LogFormat::Json,
            log_level: crate::LogLevel::Info,
        })
        .unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let router = start_grpc_server_task(tx, config).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        let task = tokio::spawn(async move {
            router.serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown.cancelled()).await.unwrap();
        });
        let client =
            HealthClient::new(Channel::from_shared(format!("http://{address}")).unwrap().connect().await.unwrap());
        (client, stop, task)
    }

    /// Builds health metadata without exposing the token in assertions or logs.
    fn request(service: &str, token: Option<&str>) -> Request<HealthCheckRequest> {
        let mut req = Request::new(HealthCheckRequest {
            service: service.into(),
        });
        if let Some(token) = token {
            req.metadata_mut().insert("authorization", token.parse().unwrap());
        }
        req
    }

    #[tokio::test]
    async fn health_check_and_watch_require_authentication() {
        let (mut client, stop, task) = server(true, "").await;
        let name = "tempgrpcd.v1.TempgrpcdService";
        for token in [None, Some("Bearer wrong")] {
            assert_eq!(
                client.check(request(name, token)).await.unwrap_err().code(),
                Code::Unauthenticated
            );
            assert_eq!(
                client.watch(request(name, token)).await.unwrap_err().code(),
                Code::Unauthenticated
            );
        }
        let response = client.check(request(name, Some("Bearer test-token"))).await.unwrap().into_inner();
        assert_eq!(response.status, ServingStatus::Serving as i32);
        assert_eq!(
            client.check(request("unknown", Some("Bearer test-token"))).await.unwrap_err().code(),
            Code::NotFound
        );
        let mut stream = client.watch(request(name, Some("Bearer test-token"))).await.unwrap().into_inner();
        assert_eq!(
            stream.message().await.unwrap().unwrap().status,
            ServingStatus::Serving as i32
        );
        drop(stream);
        stop.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn health_enforces_transport_peer_policy() {
        for (trusted, allowed) in [("127.0.0.1", true), ("192.0.2.1", false)] {
            let (mut client, stop, task) = server(false, trusted).await;
            let result = client.check(request("tempgrpcd.v1.TempgrpcdService", Some("Bearer test-token"))).await;
            if allowed {
                assert!(result.is_ok());
            } else {
                assert_eq!(result.unwrap_err().code(), Code::Unauthenticated);
            }
            stop.cancel();
            task.await.unwrap();
        }
    }
}
