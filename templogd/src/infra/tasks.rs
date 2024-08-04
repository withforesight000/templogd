use std::sync::Arc;

use tracing::instrument;

use common;
use crate::config::Config;
use crate::controller;
use common::gateway::ambient_condition::AmbientConditionRepository;
use common::gateway::nature_remo_client::NatureRemoClient;
use crate::infra;

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
        controller::log_temp::run(
            cloned_config,
            AmbientConditionRepository::DataSource::<
                NatureRemoClient<infra::http_client::ReqwestClient>,
                common::infra::redis_client::AsyncRedisCrateClient,
            >(nature_remo_client),
            tx,
        )
        .await;
    });

    let another_cloned_config = config.clone();
    let task_which_logs_to_redis = tokio::spawn(async move {
        let redis_host = another_cloned_config.get_redis_host();
        let redis_port = another_cloned_config.get_redis_port();
        let address = format!("redis://{}:{}", redis_host, redis_port);
        let client = common::infra::redis_client::AsyncRedisCrateClient::new(&address).await;

        controller::log_to_redis::run(
            another_cloned_config,
            AmbientConditionRepository::DataStore::<
                NatureRemoClient<infra::http_client::ReqwestClient>,
                common::infra::redis_client::AsyncRedisCrateClient,
            >(client),
            rx,
        )
        .await;
    });

    _ = tokio::join!(
        task_which_accesses_to_nature_remo_api,
        task_which_logs_to_redis
    );
}
