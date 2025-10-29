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
        defer!{info!("Ended")}

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
    async fn get_with_bearer_token(&self, url: &str, bearer_token: &str) -> Result<Value, Box<dyn std::error::Error + Send>> {
        debug!("Started");
        defer! {debug!("Ended")}

        // Avoid format! allocation by using concat
        let auth_header = ["Bearer ", bearer_token].concat();
        let response = self.client.get(url).header("Authorization", auth_header).send().await;
        match response {
            Ok(response) => match ReqwestClient::handle_response(response).await {
                Ok(body) => Ok(body),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send>),
            },
            Err(e) => Err(Box::new(e)),
        }
    }
}
