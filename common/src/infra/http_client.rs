use async_trait::async_trait;
use reqwest::StatusCode;
use scopeguard::defer;
use serde_json::Value;
use tracing::{debug, info, instrument};

use crate::gateway::interface::http_client::HttpClient;
use crate::infra::http_client::errors::ClientError;

pub mod errors;
// #[cfg(test)]
// use mockall::{automock, predicate::*};
// use serde_json::Value;
// use std::fmt;

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
    #[instrument(parent = None)]
    pub fn new() -> ReqwestClient {
        info!("Started");
        defer! {info!("Ended")}

        ReqwestClient {
            client: reqwest::Client::new(),
        }
    }

    #[instrument(parent = None)]
    async fn handle_response(response: reqwest::Response) -> Result<Value, ClientError> {
        debug!("Started");
        defer! {debug!("Ended")}

        match response.status() {
            StatusCode::OK => {
                let body = response.json().await;
                match body {
                    Ok(body) => Ok(body),
                    Err(error) => Err(ClientError::BodyError(error)),
                }
            }
            other => Err(ClientError::StatusCodeError(other, response.text().await.unwrap())),
        }
    }
}

#[async_trait]
impl HttpClient for ReqwestClient {
    #[instrument(parent = None)]
    async fn get_with_bearer_token(
        &self,
        url: &str,
        bearer_token: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send>> {
        debug!("Started");
        defer! {debug!("Ended")}

        let response = self.client.get(url).header("Authorization", format!("Bearer {}", bearer_token)).send().await;
        match response {
            Ok(response) => match ReqwestClient::handle_response(response).await {
                Ok(body) => Ok(body),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send>),
            },
            Err(e) => Err(Box::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::http_client::errors::ClientError;
    use httpmock::{Method, MockServer};
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
        let err = client.get_with_bearer_token(&format!("{}/fail", server.base_url()), "token").await.unwrap_err();

        let client_err = err.downcast::<ClientError>().unwrap();
        match *client_err {
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
        let err =
            client.get_with_bearer_token(&format!("{}/invalid-json", server.base_url()), "token").await.unwrap_err();

        let client_err = err.downcast::<ClientError>().unwrap();
        match *client_err {
            ClientError::BodyError(_) => {}
            _ => panic!("expected body error"),
        }
    }
}
