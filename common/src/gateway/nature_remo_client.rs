use std::error::Error;
use std::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use scopeguard::defer;
use serde_json::Value;
use tracing::{debug, info, instrument};

use crate::gateway::interface::http_client::HttpClient;
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::nature_remo::NatureRemo;

pub struct NatureRemoClient<T: HttpClient> {
    http_client: T,
    api_token: String,
    base_address: String,
    device_id: String,
}

impl<T: HttpClient + Debug> Debug for NatureRemoClient<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("client", &self.http_client)
            .field("api_token", &"<MASKED>")
            .field("base_address", &self.base_address)
            .finish()
    }
}

impl<T: HttpClient> NatureRemoClient<T> {
    #[instrument(parent = None, skip(http_client))]
    pub fn new(http_client: T, api_token: String, base_address: String, device_id: String) -> NatureRemoClient<T> {
        info!("Started");
        defer! {info!("Ended")}

        NatureRemoClient {
            http_client,
            api_token,
            base_address,
            device_id,
        }
    }

    #[instrument(parent = None, skip(self))]
    async fn get_devices(&self) -> Result<Value, Box<dyn Error + Send>> {
        debug!("Started");
        defer! {debug!("Ended")}

        let url = format!("{}/1/devices", self.base_address);
        self.http_client.get_with_bearer_token(&url, self.api_token.as_str()).await
    }
}

#[async_trait]
impl<T: HttpClient + Sync> NatureRemo for NatureRemoClient<T> {
    #[instrument(parent = None, skip(self))]
    async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error + Send>> {
        debug!("Started");
        defer! {debug!("Ended")}

        let devices = self.get_devices().await;
        // info!("Devices: {:?}", devices);

        match devices {
            Ok(devices) => {
                debug!("Devices: {:?}", devices);
                // find a hashmap with the key "id" that has the value of device_id
                let device =
                    devices.as_array().unwrap().iter().find(|d| d["id"].as_str().unwrap() == self.device_id).unwrap();
                Ok(ambient_condition::new(
                    device["newest_events"]["te"]["val"].as_f64().unwrap(),
                    device["newest_events"]["hu"]["val"].as_f64().unwrap(),
                    device["newest_events"]["il"]["val"].as_f64().unwrap(),
                ))
            }
            Err(error) => {
                info!("Failed to get devices: {}", error);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use serde_json::json;

    mock! {
        #[derive(Debug)]
        pub HttpClient {}

        #[async_trait::async_trait]
        impl HttpClient for HttpClient {
            async fn get_with_bearer_token(
                &self,
                url: &str,
                bearer_token: &str,
            ) -> Result<Value, Box<dyn std::error::Error + Send>>;
        }
    }

    #[tokio::test]
    async fn fetch_ambient_condition_returns_device_match() {
        let mut http = MockHttpClient::new();
        http.expect_get_with_bearer_token().returning(|url, token| {
            assert!(url.contains("/1/devices"));
            assert_eq!(token, "api-token");
            Ok(json!([
                {
                    "id": "device-1",
                    "newest_events": {
                        "te": {"val": 21.0},
                        "hu": {"val": 44.0},
                        "il": {"val": 88.0}
                    }
                },
                {
                    "id": "target-device",
                    "newest_events": {
                        "te": {"val": 22.5},
                        "hu": {"val": 50.5},
                        "il": {"val": 99.0}
                    }
                }
            ]))
        });

        let client = NatureRemoClient::new(
            http,
            "api-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );

        let condition = client.fetch_ambient_condition().await.unwrap();
        assert!((condition.get_temperature() - 22.5).abs() < f64::EPSILON);
        assert!((condition.get_humidity() - 50.5).abs() < f64::EPSILON);
        assert!((condition.get_illumination() - 99.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn fetch_ambient_condition_forwards_error() {
        let mut http = MockHttpClient::new();
        http.expect_get_with_bearer_token()
            .returning(|_, _| Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "boom"))));

        let client = NatureRemoClient::new(
            http,
            "api-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );

        let result = client.fetch_ambient_condition().await;
        assert!(result.is_err());
    }

    #[test]
    fn debug_does_not_expose_token() {
        let http = MockHttpClient::new();
        let client = NatureRemoClient::new(
            http,
            "secret-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );

        let debug = format!("{:?}", client);
        assert!(debug.contains("https://example"));
        assert!(debug.contains("<MASKED>"));
        assert!(!debug.contains(": \"secret-token\""));
    }
}
