use std::collections::HashMap;

use serde_json::Value;

use crate::infra::http_client::HttpClient;
use crate::model;
pub struct NatureRemoClient<T: HttpClient> {
    client: T,
    api_token: String,
    base_address: String,
}

impl<T: HttpClient> NatureRemoClient<T> {
    pub fn new(
        client: T,
        api_token: String,
        base_address: String,
    ) -> NatureRemoClient<T> {
        NatureRemoClient {
            client,
            api_token,
            base_address,
        }
    }

    pub async fn get_devices(&self) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
        let url = format!("{}/1/devices", self.base_address);
        let response = self
            .client
            .get_with_bearer_token(&url, self.api_token.as_str()).await;
        return response;
    }
}

impl<T: HttpClient> model::repository::temperature::TemperatureRepository for NatureRemoClient<T>{
    fn get_temperature(&self) -> Result<f32, Box<dyn std::error::Error>> {
        // let devices = self.get_devices();
        let temperature = Ok(23.4);
        return temperature;
    }
}
