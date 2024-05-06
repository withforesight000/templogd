use redis::Value;

pub trait AmbientCondition {
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error>>;

    async fn save_ambient_condition(
        &mut self,
        ambient_condition: common::model::ambient_condition::AmbientCondition,
    ) -> Result<Value, impl std::error::Error>;
}
