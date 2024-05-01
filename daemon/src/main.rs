use std::sync::Arc;

use common::logger;
use tokio::sync::Mutex;

mod controller;
mod infra;
mod gateway;
mod model;
mod usecase;
// static PROCESS_NAME: &str = "templogd";

#[tokio::main]
async fn main() {
    // TODO: Add command line argument parsing
    // TODO: consoder how to initialize logger
    let logger = Arc::new(Mutex::new(logger::new(logger::LoggerType::STDOUT)));

    infra::tasks::run(logger.clone()).await;
    logger.lock().await.info("exiting...");
}
