use clap::Parser;
use tracing::info;

mod config;
mod controller;
mod infra;
mod gateway;
mod model;
mod usecase;
// static PROCESS_NAME: &str = "templogd";

/// templogd
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// API token for the Nature Remo API
    #[arg(short, long, required = true, env = "TEMPLOGD_NATURE_REMO_API_TOKEN")]
    api_token: String,

    /// Device ID for the Nature Remo device
    #[arg(short, long, required = true, env = "TEMPLOGD_NATURE_REMO_DEVICE_ID")]
    device_id: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = config::new(args);

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let number_of_yaks = 3;
    // this creates a new event, outside of any spans.
    info!(number_of_yaks, "preparing to shave yaks");

    infra::tasks::run(config).await;
    info!("exiting...");

}
