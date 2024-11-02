use clap::Parser;
use tracing::info;

mod config;
mod controller;
mod infra;
mod usecase;
// static PROCESS_NAME: &str = "templogd";

/// templogd
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// API token for the Nature Remo API
    #[arg(long, required = true, env = "TEMPLOGD_NATURE_REMO_API_TOKEN")]
    api_token: String,

    /// Device ID for the Nature Remo device
    #[arg(long, required = true, env = "TEMPLOGD_NATURE_REMO_DEVICE_ID")]
    device_id: String,

    /// Device ID for the Nature Remo device
    #[arg(long, required = true, env = "TEMPLOGD_REDIS_HOST")]
    redis_host: String,

    /// Device ID for the Nature Remo device
    #[arg(long, default_value_t = 6379, env = "TEMPLOGD_REDIS_PORT")]
    redis_port: i32,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    info!("starting templogd...");

    let args = Args::parse();
    let config = config::new(args);
    info!("config loaded");

    infra::tasks::run(config).await;
    info!("exiting...");
}
