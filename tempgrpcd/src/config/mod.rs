use std::fmt;
use std::sync::Arc;

use crate::TempgrpcdArgs;

pub struct Config {
    server_bind_address: String,
    server_port: String,
    bearer_token: String,
    redis_host: String,
    redis_port: i32,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").field("bearer_token", &"<MASKED>").finish()
    }
}

pub fn new(args: TempgrpcdArgs) -> Arc<Config> {
    let server_bind_address = args.server_bind_address;
    let server_port = args.server_port;
    let bearer_token = args.bearer_token;
    let redis_host = args.redis_host;
    let redis_port = args.redis_port;

    Arc::new(Config {
        server_bind_address,
        server_port,
        bearer_token,
        redis_host,
        redis_port,
    })
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
            log_format: crate::LogFormat::Json,
            log_level: crate::LogLevel::Info,
        }
    }

    #[test]
    fn getters_return_values() {
        let cfg = super::new(args());
        assert_eq!(cfg.get_server_bind_address(), "0.0.0.0");
        assert_eq!(cfg.get_server_port(), "50051");
        assert_eq!(cfg.get_bearer_token(), "token");
        assert_eq!(cfg.get_redis_host(), "localhost");
        assert_eq!(cfg.get_redis_port(), 6379);
    }

    #[test]
    fn debug_masks_bearer_token() {
        let cfg = super::new(args());
        let debug = format!("{:?}", cfg.as_ref());
        assert!(debug.contains("<MASKED>"));
        assert!(!debug.contains(": \"token\""));
    }
}
