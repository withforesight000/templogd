use redis::{RedisError, Value};

pub trait DataStore {
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: common::model::ambient_condition::AmbientCondition,
    ) -> Result<Value, RedisError>;
}
