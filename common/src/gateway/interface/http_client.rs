use async_trait::async_trait;
use serde_json::Value;

use crate::infra::http_client::errors::ClientError;

/// Provides the outbound HTTP operations used by external-service gateways.
#[async_trait]
pub trait HttpClient {
    /// Fetches a JSON response using a bearer token without exposing the token in diagnostics.
    async fn get_with_bearer_token(&self, url: &str, bearer_token: &str) -> Result<Value, ClientError>;

    /// Sends a JSON POST request and decodes its JSON response.
    async fn post_json(&self, url: &str, body: &Value) -> Result<Value, ClientError>;

    /// Sends a GET request with one custom header and decodes its JSON response.
    async fn get_with_header(&self, url: &str, header_name: &str, header_value: &str) -> Result<Value, ClientError>;
}
