use std::fmt::Debug;
use std::sync::Arc;

use tracing::{info, instrument};

use crate::config::Config;
use crate::model::repository::temperature::TemperatureRepository;

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>, client: impl TemperatureRepository + Debug) {
    loop {
        let temperature = client.get_temperature(config.get_device_id()).await.unwrap();
        info!("Temperature: {}", temperature);
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}
