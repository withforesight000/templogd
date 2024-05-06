use std::collections::HashMap;

use redis::ToRedisArgs;

pub trait AmbientCondition {
    async fn fetch_ambient_conditions_between_start_and_end(
        &mut self,
        start: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
        end: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
    ) -> Result<
        HashMap<String, common::model::ambient_condition::AmbientCondition>,
        redis::RedisError,
    >;
}
