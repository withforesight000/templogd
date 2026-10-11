use std::{collections::HashSet, net::IpAddr};

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
    #[error("failed to build the gRPC reflection service")]
    BuildReflection(#[source] tonic_reflection::server::Error),
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
    trusted_proxy_ips: HashSet<IpAddr>,
    allow_direct_clients: bool,
}

impl AuthInterceptor {
    /// Creates an interceptor after validating the configured bearer token as gRPC metadata.
    pub(super) fn new(
        token: &str,
        trusted_proxy_ips: &HashSet<IpAddr>,
        allow_direct_clients: bool,
    ) -> Result<Self, InvalidMetadataValue> {
        let token = format!("Bearer {token}").parse()?;
        Ok(Self {
            token,
            trusted_proxy_ips: trusted_proxy_ips.clone(),
            allow_direct_clients,
        })
    }
}

impl Interceptor for AuthInterceptor {
    #[instrument(level = "info", name = "infra.authenticate", skip_all)]
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let peer = req.remote_addr().map(|address| normalize_ip(address.ip()));
        if !self.allow_direct_clients && !peer.is_some_and(|ip| self.trusted_proxy_ips.contains(&ip)) {
            warn!(reason = "untrusted_transport_peer", peer = ?peer, "gRPC authentication failed");
            return Err(Status::unauthenticated("No valid auth token"));
        }

        match req.metadata().get("authorization") {
            Some(token) if self.token == token => Ok(req),
            _ => {
                warn!(reason = "invalid_bearer_token", peer = ?peer, "gRPC authentication failed");
                Err(Status::unauthenticated("No valid auth token"))
            }
        }
    }
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(value) => value.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(value)),
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn interceptor_allows_valid_token() {
        let mut interceptor = AuthInterceptor::new("secret", &HashSet::new(), true).unwrap();
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer secret").unwrap());
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn interceptor_rejects_invalid_token() {
        let mut interceptor = AuthInterceptor::new("secret", &HashSet::new(), true).unwrap();
        let req = Request::new(());
        let err = interceptor.call(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_rejects_invalid_configured_token() {
        assert!(AuthInterceptor::new("secret\n", &HashSet::new(), true).is_err());
    }

    #[test]
    fn interceptor_rejects_missing_peer_in_strict_mode() {
        let trusted = HashSet::from(["192.168.1.1".parse().unwrap()]);
        let mut interceptor = AuthInterceptor::new("secret", &trusted, false).unwrap();
        let mut req = Request::new(());
        req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer secret").unwrap());

        assert_eq!(interceptor.call(req).unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn interceptor_allows_trusted_transport_peer() {
        use tonic::transport::server::TcpConnectInfo;

        let trusted = HashSet::from(["192.168.1.1".parse().unwrap()]);
        let mut interceptor = AuthInterceptor::new("secret", &trusted, false).unwrap();
        let mut req = Request::new(());
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some("192.168.1.1:41234".parse().unwrap()),
        });
        req.metadata_mut().insert("authorization", MetadataValue::try_from("Bearer secret").unwrap());

        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn normalizes_ipv4_mapped_ipv6_transport_peers() {
        assert_eq!(
            normalize_ip("::ffff:192.168.1.1".parse().unwrap()),
            "192.168.1.1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            normalize_ip("2001:db8::1".parse().unwrap()),
            "2001:db8::1".parse::<IpAddr>().unwrap()
        );
    }
}
