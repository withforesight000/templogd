use tonic::{
    Request, Status,
    metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
    service::Interceptor,
};
use tracing::{instrument, warn};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServerError {
    #[error("configured bearer token is invalid")]
    InvalidBearerToken(#[source] InvalidMetadataValue),
    #[error("failed to render the Redis sampling function")]
    RenderSamplingFunction(#[source] askama::Error),
    #[error("failed to connect to Redis")]
    ConnectRedis(#[source] redis::RedisError),
    #[error("failed to load the Redis sampling function")]
    LoadSamplingFunction(#[source] redis::RedisError),
    #[error("configured gRPC bind address is invalid")]
    InvalidBindAddress(#[source] std::net::AddrParseError),
    #[error("gRPC server failed")]
    ServeGrpc(#[source] tonic::transport::Error),
    #[error("{task} background task failed")]
    BackgroundTask {
        task: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("{task} background task stopped unexpectedly")]
    BackgroundTaskStopped { task: &'static str },
}

#[derive(Clone)]
pub(super) struct AuthInterceptor {
    token: AsciiMetadataValue,
}

impl AuthInterceptor {
    /// Creates an interceptor after validating the configured bearer token as gRPC metadata.
    pub(super) fn new(token: &str) -> Result<Self, InvalidMetadataValue> {
        let token = format!("Bearer {token}").parse()?;
        Ok(Self { token })
    }
}

impl Interceptor for AuthInterceptor {
    #[instrument(level = "info", name = "infra.authenticate", skip_all)]
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        match req.metadata().get("authorization") {
            Some(token) if self.token == token => Ok(req),
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
    use tonic::metadata::MetadataValue;

    #[test]
    fn interceptor_allows_valid_token() {
        let mut interceptor = AuthInterceptor::new("secret").unwrap();
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer secret").unwrap());
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn interceptor_rejects_invalid_token() {
        let mut interceptor = AuthInterceptor::new("secret").unwrap();
        let req = Request::new(());
        let err = interceptor.call(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_rejects_invalid_configured_token() {
        assert!(AuthInterceptor::new("secret\n").is_err());
    }
}
