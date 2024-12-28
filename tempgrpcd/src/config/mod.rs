use std::fmt;
use std::sync::Arc;

use crate::TempgrpcdArgs;

pub struct Config {
    server_bind_address: String,
    server_port: String,
    redis_host: String,
    redis_port: i32,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").field("api_token", &"<MASKED>").finish()
    }
}

pub fn new(args: TempgrpcdArgs) -> Arc<Config> {
    let server_bind_address = args.server_bind_address;
    let server_port = args.server_port;
    let redis_host = args.redis_host;
    let redis_port = args.redis_port;

    Arc::new(Config {
        server_bind_address,
        server_port,
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
                server_bind_address: "".to_string(),
                server_port: "".to_string(),
                redis_host: "".to_string(),
                redis_port: 0,
            }
        }
    }
}
