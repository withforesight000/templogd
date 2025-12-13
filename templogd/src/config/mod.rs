use std::fmt;
use std::sync::Arc;

use crate::TemplogdArgs;

pub struct Config {
    api_token: String,
    device_id: String,
    redis_host: String,
    redis_port: i32,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").field("api_token", &"<MASKED>").finish()
    }
}

pub fn new(args: TemplogdArgs) -> Arc<Config> {
    let api_token = args.api_token;
    let device_id = args.device_id;
    let redis_host = args.redis_host;
    let redis_port = args.redis_port;

    Arc::new(Config {
        api_token,
        device_id,
        redis_host,
        redis_port,
    })
}

impl Config {
    pub fn get_api_token(&self) -> &str {
        &self.api_token
    }

    pub fn get_device_id(&self) -> &str {
        &self.device_id
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
    use super::Config;

    impl Default for Config {
        fn default() -> Self {
            Config {
                api_token: "".to_string(),
                device_id: "".to_string(),
                redis_host: "".to_string(),
                redis_port: 0,
            }
        }
    }

    #[test]
    fn getters_return_values() {
        let cfg = Config {
            api_token: "a".into(),
            device_id: "d".into(),
            redis_host: "h".into(),
            redis_port: 42,
        };
        assert_eq!(cfg.get_api_token(), "a");
        assert_eq!(cfg.get_device_id(), "d");
        assert_eq!(cfg.get_redis_host(), "h");
        assert_eq!(cfg.get_redis_port(), 42);
    }

    #[test]
    fn debug_masks_api_token() {
        let cfg = Config {
            api_token: "secret-token".into(),
            device_id: "d".into(),
            redis_host: "h".into(),
            redis_port: 42,
        };
        let debug = format!("{:?}", cfg);
        assert!(debug.contains("<MASKED>"));
        assert!(!debug.contains("secret-token"));
    }
}
