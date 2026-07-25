use std::error::Error;
use std::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, error, instrument};

use crate::gateway::interface::http_client::HttpClient;
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::nature_remo::NatureRemo;

#[derive(Debug, Error)]
enum NatureRemoResponseError {
    #[error("Nature Remo response did not contain a device list")]
    InvalidDeviceList,
    #[error("Nature Remo response did not contain the requested device")]
    DeviceNotFound,
    #[error("Nature Remo device response did not contain valid ambient measurements")]
    InvalidAmbientMeasurements,
}

/// Retrieves the configured Nature Remo device's latest ambient readings.
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
    /// Creates a Nature Remo gateway with the API endpoint and target device.
    ///
    /// The token and device ID are retained for requests but are excluded from
    /// the gateway's debug representation and diagnostic messages.
    #[instrument(level = "info", name = "nature_remo.client_new", skip_all)]
    pub fn new(http_client: T, api_token: String, base_address: String, device_id: String) -> NatureRemoClient<T> {
        NatureRemoClient {
            http_client,
            api_token,
            base_address,
            device_id,
        }
    }

    #[instrument(level = "debug", name = "nature_remo.get_devices", skip_all, err)]
    async fn get_devices(&self) -> Result<Value, Box<dyn Error + Send>> {
        let url = format!("{}/1/devices", self.base_address);
        self.http_client
            .get_with_bearer_token(&url, self.api_token.as_str())
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send>)
    }

    /// Parses the configured device's three ambient measurements without panicking on malformed API data.
    #[instrument(level = "debug", name = "nature_remo.parse_ambient_condition", skip_all, err)]
    fn parse_ambient_condition(&self, devices: &Value) -> Result<AmbientConditionModel, NatureRemoResponseError> {
        let device = devices
            .as_array()
            .ok_or(NatureRemoResponseError::InvalidDeviceList)?
            .iter()
            .find(|device| device.get("id").and_then(Value::as_str) == Some(self.device_id.as_str()))
            .ok_or(NatureRemoResponseError::DeviceNotFound)?;

        let temperature =
            device.get("newest_events").and_then(|events| events.get("te")).and_then(|event| event.get("val"));
        let humidity =
            device.get("newest_events").and_then(|events| events.get("hu")).and_then(|event| event.get("val"));
        let illumination =
            device.get("newest_events").and_then(|events| events.get("il")).and_then(|event| event.get("val"));

        match (
            temperature.and_then(Value::as_f64),
            humidity.and_then(Value::as_f64),
            illumination.and_then(Value::as_f64),
        ) {
            (Some(temperature), Some(humidity), Some(illumination)) => {
                Ok(ambient_condition::new(temperature, humidity, illumination))
            }
            _ => Err(NatureRemoResponseError::InvalidAmbientMeasurements),
        }
    }
}

#[async_trait]
impl<T: HttpClient + Sync> NatureRemo for NatureRemoClient<T> {
    #[instrument(level = "info", name = "nature_remo.fetch_ambient_condition", skip_all, err)]
    async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error + Send>> {
        let devices = match self.get_devices().await {
            Ok(devices) => devices,
            Err(error) => {
                error!(error = %error, operation = "nature_remo.get_devices", "Nature Remo device request failed");
                return Err(error);
            }
        };

        match self.parse_ambient_condition(&devices) {
            Ok(condition) => {
                debug!(
                    operation = "nature_remo.parse_ambient_condition",
                    "Nature Remo device data parsed"
                );
                Ok(condition)
            }
            Err(error) => {
                error!(error = %error, operation = "nature_remo.parse_ambient_condition", "Nature Remo device data was invalid");
                Err(Box::new(error))
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
            ) -> Result<Value, crate::infra::http_client::errors::ClientError>;

            async fn post_json(
                &self,
                url: &str,
                body: &Value,
            ) -> Result<Value, crate::infra::http_client::errors::ClientError>;

            async fn get_with_header(
                &self,
                url: &str,
                header_name: &str,
                header_value: &str,
            ) -> Result<Value, crate::infra::http_client::errors::ClientError>;
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
        http.expect_get_with_bearer_token().returning(|_, _| {
            Err(crate::infra::http_client::errors::ClientError::StatusCodeError(
                reqwest::StatusCode::BAD_GATEWAY,
                "boom".into(),
            ))
        });

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
    fn rejects_invalid_device_list() {
        let client = NatureRemoClient::new(
            MockHttpClient::new(),
            "api-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );

        let error = client.parse_ambient_condition(&json!({"devices": []})).unwrap_err();
        assert!(matches!(error, NatureRemoResponseError::InvalidDeviceList));
    }

    #[test]
    fn rejects_missing_device() {
        let client = NatureRemoClient::new(
            MockHttpClient::new(),
            "api-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );

        let error = client.parse_ambient_condition(&json!([])).unwrap_err();
        assert!(matches!(error, NatureRemoResponseError::DeviceNotFound));
    }

    #[test]
    fn rejects_invalid_ambient_measurements() {
        let client = NatureRemoClient::new(
            MockHttpClient::new(),
            "api-token".to_string(),
            "https://example".to_string(),
            "target-device".to_string(),
        );
        let devices = json!([{"id": "target-device", "newest_events": {"te": {"val": 22.5}}}]);

        let error = client.parse_ambient_condition(&devices).unwrap_err();
        assert!(matches!(error, NatureRemoResponseError::InvalidAmbientMeasurements));
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
