use crate::model;

pub trait AmbientCondition {
    async fn get_temperature(
        &self,
        device_id: &str,
    ) -> Result<model::ambient_codition::AmbientCondition, Box<dyn std::error::Error>>;
}
