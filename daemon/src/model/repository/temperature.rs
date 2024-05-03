pub trait TemperatureRepository {
    async fn get_temperature(&self, device_id: &str) -> Result<f64, Box<dyn std::error::Error>>;
}
