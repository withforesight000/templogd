mod controller;
mod infra;
mod pb;
mod usecase;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    infra::server::run().await;
    tracing::info!("exiting...");
}
