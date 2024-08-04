use std::marker::{Send, Sync};

use redis::{RedisError, ToRedisArgs};

pub trait Redis {
    async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        items: &[(
            impl ToRedisArgs + Send + Sync,
            impl ToRedisArgs + Send + Sync,
        )],
    ) -> Result<redis::Value, RedisError>;

    async fn xrange(
        &mut self,
        key: &str,
        start: impl ToRedisArgs + Send + Sync,
        end: impl ToRedisArgs + Send + Sync,
    ) -> Result<redis::Value, RedisError>;
}
