use std::fmt::Debug;
use std::sync::Arc;

use tracing::instrument;

use crate::config::Config;
use crate::model::repository::temperature::TemperatureRepository;
use crate::usecase;

#[instrument(parent = None)]
pub async fn run(config: Arc<Config>, client: impl TemperatureRepository + Debug) {
    usecase::log_temp::run(config, client).await;
}
