use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;
use tracing::instrument;

use crate::gateway::interface::http_client::HttpClient;
use crate::infra::http_client::errors::ClientError;

pub mod errors;

const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ERROR_BODY_PREVIEW_BYTES: usize = 512;

/// Sends JSON HTTP requests with bounded failure diagnostics.
#[derive(Debug)]
pub struct ReqwestClient {
    client: reqwest::Client,
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestClient {
    /// Creates a client that applies the request timeout at each HTTP call.
    pub fn new() -> ReqwestClient {
        ReqwestClient {
            client: reqwest::Client::new(),
        }
    }

    /// Converts an HTTP response into JSON while classifying status and body failures.
    #[instrument(name = "http.handle_response", skip_all, err)]
    async fn handle_response(response: reqwest::Response) -> Result<Value, ClientError> {
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.map_err(|source| ClientError::ResponseBody { status, source })?;
            return Err(ClientError::StatusCodeError(status, body_preview(&body)));
        }

        response.json().await.map_err(classify_response_error)
    }
}

/// Classifies errors raised while sending an HTTP request.
fn classify_request_error(error: reqwest::Error) -> ClientError {
    if error.is_timeout() {
        ClientError::Timeout(error)
    } else {
        ClientError::Request(error)
    }
}

/// Classifies errors raised while decoding a successful HTTP response.
fn classify_response_error(error: reqwest::Error) -> ClientError {
    if error.is_timeout() {
        ClientError::Timeout(error)
    } else {
        ClientError::Body(error)
    }
}

/// Keeps only a bounded, UTF-8-safe preview of an error response body.
fn body_preview(body: &str) -> String {
    let bytes = body.as_bytes();
    let preview = &bytes[..bytes.len().min(MAX_ERROR_BODY_PREVIEW_BYTES)];
    let mut preview = String::from_utf8_lossy(preview).into_owned();
    if bytes.len() > MAX_ERROR_BODY_PREVIEW_BYTES {
        preview.push_str("...");
    }
    preview
}

#[async_trait]
impl HttpClient for ReqwestClient {
    #[instrument(name = "http.get_with_bearer_token", skip_all, err)]
    async fn get_with_bearer_token(&self, url: &str, bearer_token: &str) -> Result<Value, ClientError> {
        let response = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {}", bearer_token))
            .timeout(HTTP_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(classify_request_error)?;
        Self::handle_response(response).await
    }

    #[instrument(name = "http.post_json", skip_all, err)]
    async fn post_json(&self, url: &str, body: &Value) -> Result<Value, ClientError> {
        let response = self
            .client
            .post(url)
            .json(body)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(classify_request_error)?;
        Self::handle_response(response).await
    }

    #[instrument(name = "http.get_with_header", skip_all, err)]
    async fn get_with_header(&self, url: &str, header_name: &str, header_value: &str) -> Result<Value, ClientError> {
        let response = self
            .client
            .get(url)
            .header(header_name, header_value)
            .timeout(HTTP_REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(classify_request_error)?;
        Self::handle_response(response).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::http_client::errors::ClientError;
    use httpmock::{Method, MockServer};
    use reqwest::StatusCode;
    use serde_json::json;
    use std::net::TcpListener;

    fn can_bind_localhost() -> bool {
        match TcpListener::bind(("127.0.0.1", 0)) {
            Ok(listener) => {
                drop(listener);
                true
            }
            Err(err) => {
                eprintln!("Skipping HTTP client test because binding to localhost failed: {}", err);
                false
            }
        }
    }

    #[tokio::test]
    async fn get_with_bearer_token_returns_json_body() {
        if !can_bind_localhost() {
            return;
        }
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(Method::GET).path("/devices").header("Authorization", "Bearer token");
            then.status(200).json_body(json!({"ok": true}));
        });

        let client = ReqwestClient::new();
        let result = client.get_with_bearer_token(&format!("{}/devices", server.base_url()), "token").await;

        assert_eq!(result.unwrap()["ok"], true);
        mock.assert();
    }

    #[tokio::test]
    async fn get_with_bearer_token_handles_status_error() {
        if !can_bind_localhost() {
            return;
        }
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET).path("/fail");
            then.status(500).body("boom");
        });

        let client = ReqwestClient::new();
        let client_err =
            client.get_with_bearer_token(&format!("{}/fail", server.base_url()), "token").await.unwrap_err();

        match client_err {
            ClientError::StatusCodeError(status, ref body) => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(body, "boom");
            }
            _ => panic!("unexpected error"),
        }
    }

    #[tokio::test]
    async fn get_with_bearer_token_handles_body_error() {
        if !can_bind_localhost() {
            return;
        }
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET).path("/invalid-json");
            then.status(200).body("not-json");
        });

        let client = ReqwestClient::new();
        let client_err =
            client.get_with_bearer_token(&format!("{}/invalid-json", server.base_url()), "token").await.unwrap_err();

        match client_err {
            ClientError::Body(_) => {}
            _ => panic!("expected body error"),
        }
    }

    #[tokio::test]
    async fn accepts_non_200_success_and_supports_post_and_custom_header_requests() {
        if !can_bind_localhost() {
            return;
        }
        let server = MockServer::start();
        let post = server.mock(|when, then| {
            when.method(Method::POST).path("/login").json_body(json!({"role_id": "role"}));
            then.status(201).json_body(json!({"ok": true}));
        });
        let get = server.mock(|when, then| {
            when.method(Method::GET).path("/secret").header("X-Token", "token");
            then.status(200).json_body(json!({"secret": true}));
        });

        let client = ReqwestClient::new();
        let post_response =
            client.post_json(&format!("{}/login", server.base_url()), &json!({"role_id": "role"})).await.unwrap();
        let get_response =
            client.get_with_header(&format!("{}/secret", server.base_url()), "X-Token", "token").await.unwrap();

        assert_eq!(post_response["ok"], true);
        assert_eq!(get_response["secret"], true);
        post.assert();
        get.assert();
    }

    #[tokio::test]
    async fn bounds_non_success_body_preview_and_hides_it_from_display() {
        if !can_bind_localhost() {
            return;
        }
        let server = MockServer::start();
        let body = "sensitive-error-details".repeat(100);
        server.mock(|when, then| {
            when.method(Method::GET).path("/fail");
            then.status(502).body(body.clone());
        });

        let client = ReqwestClient::new();
        let error = client.get_with_bearer_token(&format!("{}/fail", server.base_url()), "token").await.unwrap_err();

        match error {
            ClientError::StatusCodeError(status, preview) => {
                assert_eq!(status, StatusCode::BAD_GATEWAY);
                assert!(preview.len() <= 515);
                assert!(format!("{status}").contains("502"));
                assert_eq!(
                    format!("{}", ClientError::StatusCodeError(status, preview)),
                    "HTTP response returned status 502 Bad Gateway"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
