use std::collections::HashMap;

use async_trait::async_trait;
use redis::{RedisError, ToRedisArgs};

use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

#[async_trait]
pub trait DataStoreRepository {
    async fn fetch_ambient_conditions<
        T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
    >(
        &mut self,
        start: T,
        end: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError>;

    async fn fetch_ambient_conditions_with_sampling<
        T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
    >(
        &mut self,
        start: T,
        end: T,
        sampling: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError>;

    // save an ambient condition to redis
    // TODO: fix method signature
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: AmbientConditionModel,
    ) -> Result<redis::Value, RedisError>;
}
