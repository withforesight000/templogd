use std::error::Error;
use std::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, info};

use crate::gateway::interface::{http_client::HttpClient, nature_remo::NatureRemo};
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};

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
    pub fn new(http_client: T, api_token: String, base_address: String, device_id: String) -> NatureRemoClient<T> {
        NatureRemoClient {
            http_client,
            api_token,
            base_address,
            device_id,
        }
    }

    async fn get_devices(&self) -> Result<Value, Box<dyn Error>> {
        let url = format!("{}/1/devices", self.base_address);
        self.http_client.get_with_bearer_token(&url, self.api_token.as_str()).await
    }
}

#[async_trait]
impl<T: HttpClient + Sync> NatureRemo for NatureRemoClient<T> {
    async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error>> {
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
