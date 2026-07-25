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
use tracing::{Instrument, error, info, info_span, instrument};

use crate::{config::Config, controller, infra::auth::ServerError};
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
            let result = initialize_datastore(config).await;
            match result {
                Ok(datastore_client) => {
                    controller::fetch_from_redis::run(datastore_client, rx, cancellation_token).await;
                }
                Err(error) => {
                    error!(error = %error, "Redis worker initialization failed");
                    cancellation_token.cancel();
                }
            }
        }
        .instrument(task_span),
    )
}

/// Connects to Redis and installs the sampling function required by the worker.
async fn initialize_datastore(
    config: Arc<Config>,
) -> Result<DataStore<common::infra::async_redis_client::AsyncRedisCrateClient>, ServerError> {
    #[derive(Template)]
    #[template(path = "xrange_with_sampling.lua.j2")]
    struct XRANGEWithSamplingTemplate<'a> {
        function_name: &'a str,
    }

    let xrange_with_sampling_code = XRANGEWithSamplingTemplate {
        function_name: REDIS_XRANGE_WITH_SAMPLING,
    }
    .render()
    .map_err(ServerError::RenderSamplingFunction)?;

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
        .map_err(ServerError::LoadSamplingFunction)?;

    Ok(datastore_client)
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
                },
                _ = cancellation_token.cancelled() => {
                    info!(reason = "shutdown_completed", "Signal handler stopped");
                }
            }
        }
        .instrument(info_span!("infra.signal_handler.task")),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datastore_initialization_errors_preserve_their_sources() {
        let render_error = ServerError::RenderSamplingFunction(askama::Error::Fmt);
        assert!(std::error::Error::source(&render_error).is_some());

        let redis_error = redis::RedisError::from((redis::ErrorKind::Client, "load failed"));
        let load_error = ServerError::LoadSamplingFunction(redis_error);
        assert!(std::error::Error::source(&load_error).unwrap().to_string().contains("load failed"));
    }
}
