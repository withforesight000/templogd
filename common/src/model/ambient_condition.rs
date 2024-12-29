use scopeguard::defer;
use tracing::{info, instrument};

#[derive(Debug)]
pub struct AmbientCondition {
    temperature: f64,
    humidity: f64,
    illumination: f64,
}

pub fn new(temperature: f64, humidity: f64, illumination: f64) -> AmbientCondition {
    AmbientCondition {
        temperature,
        humidity,
        illumination,
    }
}

impl AmbientCondition {
    #[instrument(parent = None)]
    pub fn get_temperature(&self) -> f64 {
        info!("Started");
        defer! {info!("Ended")}

        self.temperature
    }

    #[instrument(parent = None)]
    pub fn get_humidity(&self) -> f64 {
        info!("Started");
        defer! {info!("Ended")}

        self.humidity
    }

    #[instrument(parent = None)]
    pub fn get_illumination(&self) -> f64 {
        info!("Started");
        defer! {info!("Ended")}

        self.illumination
    }
}
