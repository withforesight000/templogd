use std::fmt;
use std::sync::Arc;

use crate::Args;

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

pub fn new(args: Args) -> Arc<Config> {
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
