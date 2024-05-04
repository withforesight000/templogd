use redis::{aio::ConnectionManager, AsyncCommands, Value};

use crate::gateway::interface::redis_client::RedisClient;

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

impl RedisClient for AsyncRedisCrateClient {
    async fn xadd(&mut self, key: &str, id: &str, items: &[(&str, &str)]) -> Result<Value, redis::RedisError> {
        self.connection.xadd(key, id, items).await
    }
}
