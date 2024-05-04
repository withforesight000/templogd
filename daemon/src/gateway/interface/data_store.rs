use redis::{RedisError, Value};

use crate::model;

pub trait DataStore {
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: model::ambient_codition::AmbientCondition,
    ) -> Result<Value, RedisError>;
}
