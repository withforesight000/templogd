use std::sync::Arc;

use common::model::repository::nature_remo::NatureRemo;
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::Config;
use crate::usecase;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    config: Arc<Config>,
    client: impl NatureRemo,
    tx: tokio::sync::mpsc::Sender<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    usecase::log_temp::run(config, client, tx, cancellation_token).await;
}
