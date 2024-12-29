use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait HttpClient {
    async fn get_with_bearer_token(
        &self,
        url: &str,
        bearer_token: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send>>;
}
