use redis::Value;

use crate::model;

pub trait AmbientCondition {
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<model::ambient_codition::AmbientCondition, Box<dyn std::error::Error>>;

    async fn save_ambient_condition(
        &mut self,
        ambient_condition: model::ambient_codition::AmbientCondition,
    ) -> Result<Value, impl std::error::Error>;
}
