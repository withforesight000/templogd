use std::sync::Arc;

use crate::config::Config;
use crate::gateway::interface::redis_client::RedisClient;
use crate::model::channel::redis_command::RedisCommand;
use crate::usecase;

pub async fn run(config: Arc<Config>, client: impl RedisClient, rx: tokio::sync::mpsc::Receiver<RedisCommand>) {
    usecase::log_to_redis::run(config, client, rx).await;
}
