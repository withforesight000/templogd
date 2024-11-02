use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands, RedisError, ToRedisArgs, Value};

pub struct AsyncRedisCrateClient {
    connection: ConnectionManager,
}

impl AsyncRedisCrateClient {
    pub async fn new(host: &str) -> Self {
        let client = redis::Client::open(host).unwrap();
        let connection = ConnectionManager::new(client).await.unwrap();
        Self { connection }
    }
}

#[async_trait]
impl crate::gateway::interface::redis::Redis for AsyncRedisCrateClient {
    async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        items: &[(impl ToRedisArgs + Send + Sync, impl ToRedisArgs + Send + Sync)],
    ) -> Result<Value, RedisError> {
        self.connection.xadd(key, id, items).await
    }

    async fn xrange(
        &mut self,
        key: &str,
        start: impl ToRedisArgs + Send + Sync,
        end: impl ToRedisArgs + Send + Sync,
    ) -> Result<Value, RedisError> {
        self.connection.xrange(key, start, end).await
    }
}
