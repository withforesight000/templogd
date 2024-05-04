use std::fmt::Debug;
use std::sync::Arc;

use tracing::instrument;

use crate::config::Config;
use crate::model::channel::redis_command::RedisCommand;
use crate::model::repository::ambient_condition::AmbientCondition;
use crate::usecase;

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>, client: impl AmbientCondition + Debug, tx: tokio::sync::mpsc::Sender<RedisCommand>) {
    usecase::log_temp::run(config, client, tx).await;
}
