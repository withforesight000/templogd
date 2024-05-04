use std::fmt::Debug;
use std::sync::Arc;

use tracing::{info, instrument};

use crate::config::Config;
use crate::model::channel::redis_command::RedisCommand;
use crate::model::repository::ambient_condition::AmbientCondition;

#[instrument(parent = None)]
pub async fn run(
    config: Arc<Config>,
    client: impl AmbientCondition + Debug,
    tx: tokio::sync::mpsc::Sender<RedisCommand>,
) {
    loop {
        let condition = client
            .get_temperature(config.get_device_id())
            .await
            .unwrap();
        info!(
            "Temperature: {}, Humidity: {}, Illumination: {}",
            condition.get_temperature(),
            condition.get_humidity(),
            condition.get_illumination()
        );
        tx.send(RedisCommand::Xadd {
            key: "ambient_condition".to_string(),
            id: "*".to_string(),
            items: vec![
                ("temperature".to_string(), condition.get_temperature().to_string()),
                ("humidity".to_string(), condition.get_humidity().to_string()),
                ("illumination".to_string(), condition.get_illumination().to_string()),
            ],
        }).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}
