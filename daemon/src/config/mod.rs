use std::fmt;
use std::sync::Arc;

use crate::Args;

pub struct Config {
    api_token: String,
    device_id: String
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("api_token", &"<MASKED>")
            .finish()
    }
}

pub fn new(args: Args) -> Arc<Config> {
    let api_token = args.api_token;
    let device_id = args.device_id;

    Arc::new(Config {
        api_token: api_token,
        device_id: device_id
    })
}

impl Config {
    pub fn get_api_token(&self) -> &str {
        &self.api_token
    }

    pub fn get_device_id(&self) -> &str {
        &self.device_id
    }
}
