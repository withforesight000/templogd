use std::sync::Arc;

use tokio::{self, sync::Mutex};

use common::logger;

use crate::controller;

pub async fn run(logger: Arc<Mutex<Box<dyn logger::Logger>>>) {

    let task = tokio::spawn(async move {
        controller::log_temp::run(logger.clone()).await;
    });

    _ = tokio::join!(task);
}
