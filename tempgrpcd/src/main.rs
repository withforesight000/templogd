mod infra;
mod pb;
mod usecase;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    infra::server::run().await?;
    Ok(())
}
