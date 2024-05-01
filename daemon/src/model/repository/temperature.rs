pub trait TemperatureRepository {
    fn get_temperature(&self) -> Result<f32, Box<dyn std::error::Error>>;
}


