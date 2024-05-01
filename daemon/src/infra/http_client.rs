use std::collections::HashMap;

use reqwest::StatusCode;
use serde_json::Value;

use crate::infra::http_client::errors::ClientError;

pub mod errors;
// #[cfg(test)]
// use mockall::{automock, predicate::*};
// use serde_json::Value;
// use std::fmt;

pub struct ReqwestClient {
    client: reqwest::Client,
}

impl ReqwestClient {
    pub fn new() -> ReqwestClient {
        ReqwestClient {
            client: reqwest::Client::new(),
        }
    }

    async fn handle_response(
        response: reqwest::Response,
    ) -> Result<HashMap<String, Value>, ClientError> {
        match response.status() {
            StatusCode::OK => {
                let body: Result<HashMap<String, Value>, reqwest::Error> =
                    response.json::<HashMap<String, Value>>().await;
                match body {
                    Ok(body) => Ok(body),
                    Err(error) => Err(ClientError::BodyError(error)),
                }
            }
            other => Err(ClientError::StatusCodeError(
                other,
                response.text().await.unwrap(),
            )),
        }
    }
}

// #[cfg_attr(test, automock)]
pub trait HttpClient {
    async fn get_with_bearer_token(
        &self,
        url: &str,
        bearer_token: &str,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>>;
}

impl HttpClient for ReqwestClient {
    async fn get_with_bearer_token(
        &self,
        url: &str,
        bearer_token: &str,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let response = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {}", bearer_token))
            .send()
            .await;
        match response {
            Ok(response) => {
                match ReqwestClient::handle_response(response).await {
                    Ok(body) => Ok(body),
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error>),
                }
            }
            Err(e) => Err(Box::new(e)),
        }
    }
}
