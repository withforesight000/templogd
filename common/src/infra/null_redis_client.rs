use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{RedisError, ToRedisArgs, Value};

pub struct NullRedisClient {}

impl NullRedisClient {
    pub async fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl crate::gateway::interface::redis::Redis for NullRedisClient {
    async fn xadd(
        &mut self,
        _key: &str,
        _id: &str,
        _items: &[(impl ToRedisArgs + Send + Sync, impl ToRedisArgs + Send + Sync)],
    ) -> Result<Value, RedisError> {
        panic!("BUGS: This should not be called: Not implemented");
    }

    async fn xrange(
        &mut self,
        _key: &str,
        _start: impl ToRedisArgs + Send + Sync,
        _end: impl ToRedisArgs + Send + Sync,
    ) -> Result<Value, RedisError> {
        panic!("BUGS: This should not be called: Not implemented");
    }
}
