use std::sync::Arc;

use tracing::info;

use crate::config::Config;
use crate::gateway::interface::redis_client::RedisClient;
use crate::model::channel::redis_command::RedisCommand;

pub async fn run(_config: Arc<Config>, mut client: impl RedisClient, mut rx: tokio::sync::mpsc::Receiver<RedisCommand>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            RedisCommand::Xadd { key, id, items } => {
                let items_ref: Vec<(&str, &str)> = items.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                let items_slice: &[(&str, &str)] = items_ref.as_slice();
                let res = client.xadd(&key, &id, items_slice).await.unwrap();
                info!(":::Result from redis: {:?}", res)
            }
        }
    }
}
