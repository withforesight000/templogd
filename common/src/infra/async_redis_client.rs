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
    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<Value, RedisError> {
        self.connection.xadd(key, id, items).await
    }

    async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        start: T,
        end: U,
    ) -> Result<Value, RedisError> {
        self.connection.xrange(key, start, end).await
    }
}
