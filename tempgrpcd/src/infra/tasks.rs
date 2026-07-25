use std::sync::Arc;

use askama::Template;
use tokio::{
    signal::{
        self,
        unix::{SignalKind, signal},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, instrument};

use crate::{config::Config, controller};
use common::{gateway::datastore::DataStore, model::channel::datastore_operation::DatastoreOperation};

pub(super) const REDIS_XRANGE_WITH_SAMPLING: &str = "xrange_with_sampling";

/// Starts the Redis worker that loads the sampling function and handles datastore operations.
#[instrument(level = "debug", name = "infra.start_datastore_task", skip_all)]
pub(super) fn start_datastore_task(
    config: Arc<Config>,
    rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    let task_span = info_span!("infra.redis.task");
    tokio::spawn(
        async move {
            #[derive(Template)]
            #[template(path = "xrange_with_sampling.lua.j2")]
            struct XRANGEWithSamplingTemplate<'a> {
                function_name: &'a str,
            }
            let xrange_with_sampling_code = XRANGEWithSamplingTemplate {
                function_name: REDIS_XRANGE_WITH_SAMPLING,
            }
            .render()
            .expect("Failed to render template");

            let mut datastore_client = DataStore::new(
                common::infra::async_redis_client::AsyncRedisCrateClient::new(&format!(
                    "redis://{}:{}",
                    config.get_redis_host(),
                    config.get_redis_port()
                ))
                .await,
            )
            .await;
            datastore_client
                .load_function_xrange_with_sampling(&xrange_with_sampling_code)
                .await
                .expect("Failed to load Lua script for xrange with sampling");
            controller::fetch_from_redis::run(datastore_client, rx, cancellation_token).await
        }
        .instrument(task_span),
    )
}

/// Starts the signal watcher that cancels the server and worker tasks on shutdown.
#[instrument(level = "debug", name = "infra.start_signal_handler_task", skip_all)]
pub(super) async fn start_signal_handler_task(cancellation_token: CancellationToken) {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create signal");
    tokio::spawn(
        async move {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("SIGINT received");

                    cancellation_token.cancel();
                },
                _ = sigterm.recv() => {
                    info!("SIGTERM received");

                    cancellation_token.cancel();
                }
            }
        }
        .instrument(info_span!("infra.signal_handler.task")),
    );
}
