use std::fmt::{self, Debug};

use serde_json::Value;
use tracing::{debug, info};

use crate::gateway::interface::http_client::HttpClient;
use crate::gateway::interface::data_source::DataSource;
use crate::model;

pub struct NatureRemoClient<T: HttpClient> {
    client: T,
    api_token: String,
    base_address: String,
    device_id: String
}

impl<T: HttpClient + Debug> fmt::Debug for NatureRemoClient<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("client", &self.client)
            .field("api_token", &"<MASKED>")
            .field("base_address", &self.base_address)
            .finish()
    }
}

impl<T: HttpClient> NatureRemoClient<T> {
    pub fn new(client: T, api_token: String, base_address: String, device_id: String) -> NatureRemoClient<T> {
        NatureRemoClient {
            client,
            api_token,
            base_address,
            device_id,
        }
    }

    async fn get_devices(&self) -> Result<Value, Box<dyn std::error::Error>> {
        let url = format!("{}/1/devices", self.base_address);
        self.client
            .get_with_bearer_token(&url, self.api_token.as_str())
            .await
    }
}

impl<T: HttpClient> DataSource for NatureRemoClient<T> {
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<model::ambient_codition::AmbientCondition, Box<dyn std::error::Error>> {
        let devices = self.get_devices().await;
        // info!("Devices: {:?}", devices);

        match devices {
            Ok(devices) => {
                debug!("Devices: {:?}", devices);
                // find a hashmap with the key "id" that has the value of device_id
                let device = devices
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|d| d["id"].as_str().unwrap() == self.device_id)
                    .unwrap();
                Ok(model::ambient_codition::new(
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
