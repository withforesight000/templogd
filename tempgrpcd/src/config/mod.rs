use std::{collections::HashSet, fmt, net::IpAddr, sync::Arc, time::Duration};

use crate::TempgrpcdArgs;

pub struct Config {
    server_bind_address: String,
    server_port: String,
    bearer_token: String,
    redis_host: String,
    redis_port: i32,
    trusted_proxy_ips: HashSet<IpAddr>,
    allow_direct_clients: bool,
    max_result_entries: usize,
    max_samples: u64,
    max_concurrent_requests: usize,
    request_timeout: Duration,
    accept_legacy_millisecond_timestamps: bool,
}

pub const MAX_RESULT_ENTRIES_CEILING: usize = 5000;
pub const MAX_SAMPLES_CEILING: u64 = 1000;
pub const MAX_CONCURRENT_REQUESTS_CEILING: usize = 4;
pub const MAX_REQUEST_TIMEOUT_MILLISECONDS: u64 = 5000;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("trusted proxy IP list must not be empty unless direct clients are enabled")]
    MissingTrustedProxy,
    #[error("trusted proxy IP is invalid: {value}")]
    InvalidTrustedProxyIp {
        value: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("{name} must be between 1 and {ceiling}")]
    InvalidLimit { name: &'static str, ceiling: u64 },
    #[error("max_samples must not exceed max_result_entries")]
    SamplesExceedResults,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").field("bearer_token", &"<MASKED>").finish()
    }
}

/// Validates command-line configuration and builds the shared tempgrpcd settings.
///
/// The returned configuration preserves the bearer token privately and contains
/// parsed trusted peers, bounded request limits, and the request timeout. This
/// returns [`ConfigError`] when a trusted peer is malformed or missing in strict
/// mode, when a limit is zero or exceeds its safety ceiling, or when sampling
/// could request more entries than an unsampled result permits. It performs no
/// network or filesystem I/O.
pub fn new(args: TempgrpcdArgs) -> Result<Arc<Config>, ConfigError> {
    let server_bind_address = args.server_bind_address;
    let server_port = args.server_port;
    let bearer_token = args.bearer_token;
    let redis_host = args.redis_host;
    let redis_port = args.redis_port;

    let trusted_proxy_ips = args
        .trusted_proxy_ips
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            let value = value.trim();
            value.parse().map_err(|source| ConfigError::InvalidTrustedProxyIp {
                value: value.to_string(),
                source,
            })
        })
        .collect::<Result<HashSet<_>, _>>()?;
    if !args.allow_direct_clients && trusted_proxy_ips.is_empty() {
        return Err(ConfigError::MissingTrustedProxy);
    }
    validate_limit(
        "max_result_entries",
        args.max_result_entries as u64,
        MAX_RESULT_ENTRIES_CEILING as u64,
    )?;
    validate_limit("max_samples", args.max_samples, MAX_SAMPLES_CEILING)?;
    validate_limit(
        "max_concurrent_requests",
        args.max_concurrent_requests as u64,
        MAX_CONCURRENT_REQUESTS_CEILING as u64,
    )?;
    validate_limit(
        "request_timeout_milliseconds",
        args.request_timeout_milliseconds,
        MAX_REQUEST_TIMEOUT_MILLISECONDS,
    )?;
    if args.max_samples > args.max_result_entries as u64 {
        return Err(ConfigError::SamplesExceedResults);
    }

    Ok(Arc::new(Config {
        server_bind_address,
        server_port,
        bearer_token,
        redis_host,
        redis_port,
        trusted_proxy_ips,
        allow_direct_clients: args.allow_direct_clients,
        max_result_entries: args.max_result_entries,
        max_samples: args.max_samples,
        max_concurrent_requests: args.max_concurrent_requests,
        request_timeout: Duration::from_millis(args.request_timeout_milliseconds),
        accept_legacy_millisecond_timestamps: args.accept_legacy_millisecond_timestamps,
    }))
}

fn validate_limit(name: &'static str, value: u64, ceiling: u64) -> Result<(), ConfigError> {
    if value == 0 || value > ceiling {
        return Err(ConfigError::InvalidLimit { name, ceiling });
    }
    Ok(())
}

impl Config {
    pub fn get_server_bind_address(&self) -> &str {
        &self.server_bind_address
    }

    pub fn get_server_port(&self) -> &str {
        &self.server_port
    }

    pub fn get_bearer_token(&self) -> &str {
        &self.bearer_token
    }

    pub fn get_redis_host(&self) -> &str {
        &self.redis_host
    }

    pub fn get_redis_port(&self) -> i32 {
        self.redis_port
    }

    /// Returns the transport peer IPs allowed to call history RPCs in strict mode.
    pub fn get_trusted_proxy_ips(&self) -> &HashSet<IpAddr> {
        &self.trusted_proxy_ips
    }

    /// Reports whether authenticated clients may bypass transport-peer validation.
    ///
    /// This should be enabled only for explicit local development.
    pub fn allow_direct_clients(&self) -> bool {
        self.allow_direct_clients
    }

    /// Returns the maximum number of Redis entries an unsampled request may return.
    pub fn get_max_result_entries(&self) -> usize {
        self.max_result_entries
    }

    /// Returns the maximum sampling count accepted from one request.
    pub fn get_max_samples(&self) -> u64 {
        self.max_samples
    }

    /// Returns the maximum number of history RPCs admitted concurrently.
    pub fn get_max_concurrent_requests(&self) -> usize {
        self.max_concurrent_requests
    }

    /// Returns the deadline applied to each history request.
    pub fn get_request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Reports whether the temporary milliseconds-in-seconds timestamp encoding is accepted.
    pub fn accept_legacy_millisecond_timestamps(&self) -> bool {
        self.accept_legacy_millisecond_timestamps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Default for Config {
        fn default() -> Self {
            Config {
                server_bind_address: "".to_string(),
                server_port: "".to_string(),
                bearer_token: "".to_string(),
                redis_host: "".to_string(),
                redis_port: 0,
                trusted_proxy_ips: HashSet::new(),
                allow_direct_clients: true,
                max_result_entries: MAX_RESULT_ENTRIES_CEILING,
                max_samples: MAX_SAMPLES_CEILING,
                max_concurrent_requests: MAX_CONCURRENT_REQUESTS_CEILING,
                request_timeout: Duration::from_millis(MAX_REQUEST_TIMEOUT_MILLISECONDS),
                accept_legacy_millisecond_timestamps: false,
            }
        }
    }

    fn args() -> crate::TempgrpcdArgs {
        crate::TempgrpcdArgs {
            server_bind_address: "0.0.0.0".into(),
            server_port: "50051".into(),
            bearer_token: "token".into(),
            redis_host: "localhost".into(),
            redis_port: 6379,
            trusted_proxy_ips: "".into(),
            allow_direct_clients: true,
            max_result_entries: 5000,
            max_samples: 1000,
            max_concurrent_requests: 4,
            request_timeout_milliseconds: 5000,
            accept_legacy_millisecond_timestamps: false,
            log_format: crate::LogFormat::Json,
            log_level: crate::LogLevel::Info,
        }
    }

    #[test]
    fn getters_return_values() {
        let cfg = super::new(args()).unwrap();
        assert_eq!(cfg.get_server_bind_address(), "0.0.0.0");
        assert_eq!(cfg.get_server_port(), "50051");
        assert_eq!(cfg.get_bearer_token(), "token");
        assert_eq!(cfg.get_redis_host(), "localhost");
        assert_eq!(cfg.get_redis_port(), 6379);
    }

    #[test]
    fn debug_masks_bearer_token() {
        let cfg = super::new(args()).unwrap();
        let debug = format!("{:?}", cfg.as_ref());
        assert!(debug.contains("<MASKED>"));
        assert!(!debug.contains(": \"token\""));
    }

    #[test]
    fn strict_mode_requires_a_trusted_proxy() {
        let mut value = args();
        value.allow_direct_clients = false;

        assert!(matches!(super::new(value), Err(ConfigError::MissingTrustedProxy)));
    }

    #[test]
    fn parses_trusted_proxy_ips() {
        let mut value = args();
        value.allow_direct_clients = false;
        value.trusted_proxy_ips = "192.168.1.1,::1".into();

        let config = super::new(value).unwrap();

        assert!(config.get_trusted_proxy_ips().contains(&"192.168.1.1".parse().unwrap()));
        assert!(config.get_trusted_proxy_ips().contains(&"::1".parse().unwrap()));
    }

    #[test]
    fn rejects_malformed_trusted_proxy_ip() {
        let mut value = args();
        value.trusted_proxy_ips = "192.168.1.1,not-an-ip".into();

        assert!(matches!(
            super::new(value),
            Err(ConfigError::InvalidTrustedProxyIp { value, .. }) if value == "not-an-ip"
        ));
    }

    fn assert_invalid_limit(value: crate::TempgrpcdArgs, name: &'static str, ceiling: u64) {
        assert!(matches!(
            super::new(value),
            Err(ConfigError::InvalidLimit {
                name: actual_name,
                ceiling: actual_ceiling,
            }) if actual_name == name && actual_ceiling == ceiling
        ));
    }

    #[test]
    fn rejects_zero_and_above_ceiling_limits() {
        let mut value = args();
        value.max_result_entries = 0;
        assert_invalid_limit(value, "max_result_entries", MAX_RESULT_ENTRIES_CEILING as u64);

        let mut value = args();
        value.max_result_entries = MAX_RESULT_ENTRIES_CEILING + 1;
        assert_invalid_limit(value, "max_result_entries", MAX_RESULT_ENTRIES_CEILING as u64);

        let mut value = args();
        value.max_samples = 0;
        assert_invalid_limit(value, "max_samples", MAX_SAMPLES_CEILING);

        let mut value = args();
        value.max_samples = MAX_SAMPLES_CEILING + 1;
        assert_invalid_limit(value, "max_samples", MAX_SAMPLES_CEILING);

        let mut value = args();
        value.max_concurrent_requests = 0;
        assert_invalid_limit(value, "max_concurrent_requests", MAX_CONCURRENT_REQUESTS_CEILING as u64);

        let mut value = args();
        value.max_concurrent_requests = MAX_CONCURRENT_REQUESTS_CEILING + 1;
        assert_invalid_limit(value, "max_concurrent_requests", MAX_CONCURRENT_REQUESTS_CEILING as u64);

        let mut value = args();
        value.request_timeout_milliseconds = 0;
        assert_invalid_limit(value, "request_timeout_milliseconds", MAX_REQUEST_TIMEOUT_MILLISECONDS);

        let mut value = args();
        value.request_timeout_milliseconds = MAX_REQUEST_TIMEOUT_MILLISECONDS + 1;
        assert_invalid_limit(value, "request_timeout_milliseconds", MAX_REQUEST_TIMEOUT_MILLISECONDS);
    }

    #[test]
    fn rejects_samples_exceeding_result_limit() {
        let mut value = args();
        value.max_result_entries = 10;
        value.max_samples = 11;

        assert!(matches!(super::new(value), Err(ConfigError::SamplesExceedResults)));
    }
}
