use std::sync::Arc;

use tracing::instrument;

use crate::config::Config;
use crate::gateway::nature_remo_client::NatureRemoClient;
use crate::controller;

#[instrument]
pub async fn run(config: Arc<Config>) {
    // A task that logs the temperature every 5 seconds to the console
    // TODO: logs to the Redis
    let task = tokio::spawn(async move {
    let client = NatureRemoClient::new(
        crate::infra::http_client::ReqwestClient::new(),
        config.get_api_token().to_string(),
        "https://api.nature.global".to_string(),
    );
        controller::log_temp::run(config, client).await;
    });

    _ = tokio::join!(task);
}
