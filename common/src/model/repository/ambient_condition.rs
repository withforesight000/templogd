use std::collections::HashMap;
use std::error::Error;

use redis::ToRedisArgs;

use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

pub trait AmbientCondition {
    // fetch a current ambient condition from Nature Remo API
    // TODO: fix method signature
    async fn fetch_current_ambient_condition(
        &self,
    ) -> Result<AmbientConditionModel, Box<dyn Error>>;

    // save an ambient condition to redis
    // TODO: fix method signature
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: AmbientConditionModel,
    ) -> Result<redis::Value, impl Error>;

    // fetch saved ambient conditions from redis
    async fn fetch_ambient_conditions_between_start_and_end(
        &mut self,
        start: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
        end: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
    ) -> Result<
        HashMap<String, AmbientConditionModel>,
        redis::RedisError,
    >;
}
