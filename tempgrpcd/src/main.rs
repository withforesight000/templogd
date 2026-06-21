use clap::Parser;
use tracing::info;

mod config;
mod controller;
mod infra;
mod usecase;
mod validator;

/// tempgrpcd
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct TempgrpcdArgs {
    /// Server bind address
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_BIND_ADDRESS")]
    server_bind_address: String,

    /// Server port
    #[arg(long, required = true, env = "TEMPGRPCD_SERVER_PORT")]
    server_port: String,

    /// API bearer token
    #[arg(long, required = true, env = "TEMPGRPCD_BEARER_TOKEN")]
    bearer_token: String,

    /// Redis host
    #[arg(long, required = true, env = "TEMPGRPCD_REDIS_HOST")]
    redis_host: String,

    /// Redis port
    #[arg(long, default_value_t = 6379, env = "TEMPGRPCD_REDIS_PORT")]
    redis_port: i32,
}

#[tokio::main]
async fn main() {
    json_subscriber::fmt().with_max_level(tracing::Level::DEBUG).init();
    info!("starting tempgrpcd...");

    let args = TempgrpcdArgs::parse();
    let config = config::new(args);
    info!("config loaded");

    infra::server::run(config).await;
    tracing::info!("exiting...");
}
