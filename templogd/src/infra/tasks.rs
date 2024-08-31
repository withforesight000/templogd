use std::sync::Arc;

use common::infra::null_nature_remo_client::NullNatureRemoClient;
use common::infra::{async_redis_client, null_redis_client};
use tracing::instrument;

use common;
use crate::config::Config;
use crate::controller;
use common::gateway::ambient_condition::AmbientConditionRepository;
use common::infra::nature_remo_client::NatureRemoClient;

#[instrument]
pub async fn run(config: Arc<Config>) {
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    // A task that logs the temperature every 30 seconds to the console
    // TODO: logs to the Redis
    let cloned_config = config.clone();
    let task_which_accesses_to_nature_remo_api = tokio::spawn(async move {
        let nature_remo_client = NatureRemoClient::new(
            common::infra::http_client::ReqwestClient::new(),
            cloned_config.get_api_token().to_string(),
            "https://api.nature.global".to_string(),
            cloned_config.get_device_id().to_string(),
        );
        let redis_client = null_redis_client::NullRedisClient::new().await;
        let ambient_condition = AmbientConditionRepository::new(nature_remo_client, redis_client);
        controller::log_temp::run(
            cloned_config,
            ambient_condition,
            tx,
        ).await;
    });

    let another_cloned_config = config.clone();
    let task_which_logs_to_redis = tokio::spawn(async move {
        let nature_remo_client = NullNatureRemoClient::new();
        let redis_client = async_redis_client::AsyncRedisCrateClient::new(
            &format!(
                "redis://{}:{}",
                another_cloned_config.get_redis_host(),
                another_cloned_config.get_redis_port()
            ),
        ).await;
        let ambient_condition = AmbientConditionRepository::new(nature_remo_client, redis_client);

        controller::log_to_redis::run(
            another_cloned_config,
            ambient_condition,
            rx,
        ).await;
    });

    _ = tokio::join!(
        task_which_accesses_to_nature_remo_api,
        task_which_logs_to_redis
    );
}
