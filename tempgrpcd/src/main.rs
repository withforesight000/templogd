use clap::Parser;
use tracing::info;

mod config;
mod controller;
mod infra;
mod pb;
mod usecase;

/// tempgrpcd
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct TempgrpcdArgs {
    /// API token for the Nature Remo API
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_BIND_ADDRESS")]
    server_bind_address: String,

    /// Device ID for the Nature Remo device
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_PORT")]
    server_port: String,

    /// Device ID for the Nature Remo device
    #[arg(long, required = true, env = "TEMPGRPCD_REDIS_HOST")]
    redis_host: String,

    /// Device ID for the Nature Remo device
    #[arg(long, default_value_t = 6379, env = "TEMPGRPCD_REDIS_PORT")]
    redis_port: i32,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    info!("starting tempgrpcd...");

    let args = TempgrpcdArgs::parse();
    let config = config::new(args);
    info!("config loaded");

    infra::server::run(config).await;
    tracing::info!("exiting...");
}
