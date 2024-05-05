use serde_json::Value;

pub trait HttpClient {
    async fn get_with_bearer_token(
        &self,
        url: &str,
        bearer_token: &str,
    ) -> Result<Value, Box<dyn std::error::Error>>;
}
