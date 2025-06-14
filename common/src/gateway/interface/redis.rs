use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{RedisError, ToRedisArgs};

#[async_trait]
pub trait Redis {
    async fn fcall<K: ToRedisArgs + Send + Sync + 'static, A: ToRedisArgs + Send + Sync + 'static>(
        &mut self,
        function: &str,
        keys: &[K],
        args: &[A],
    ) -> Result<redis::Value, RedisError>;

    async fn function_load(&mut self, replace: bool, code: &str) -> Result<String, RedisError>;

    async fn xadd<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<redis::Value, RedisError>;

    async fn xrange<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
        &mut self,
        key: &str,
        start: T,
        end: U,
    ) -> Result<redis::Value, RedisError>;
}
