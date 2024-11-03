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
    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        _key: &str,
        _id: &str,
        _items: &[(T, U)],
    ) -> Result<Value, RedisError> {
        panic!("BUGS: This should not be called: Not implemented");
    }

    async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        _key: &str,
        _start: T,
        _end: U,
    ) -> Result<Value, RedisError> {
        panic!("BUGS: This should not be called: Not implemented");
    }
}
