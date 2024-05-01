use std::sync::Arc;

use tokio::sync::Mutex;

use common::logger;
use crate::usecase;

pub async fn run(logger: Arc<Mutex<Box<dyn logger::Logger>>>) {
    usecase::log_temp::run(logger).await;
}
