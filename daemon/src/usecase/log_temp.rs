use std::sync::Arc;

use tokio::sync::Mutex;

use common::logger;

pub async fn run(logger: Arc<Mutex<Box<dyn logger::Logger>>>) {
    logger.clone().lock().await.info("Starting tasks");
}
