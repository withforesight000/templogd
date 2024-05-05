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
    pub fn get_temperature(&self) -> f64 {
        self.temperature
    }

    pub fn get_humidity(&self) -> f64 {
        self.humidity
    }

    pub fn get_illumination(&self) -> f64 {
        self.illumination
    }
}
