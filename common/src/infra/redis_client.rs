use redis::{aio::ConnectionManager, AsyncCommands, ToRedisArgs, Value};

pub struct AsyncRedisCrateClient {
    connection: ConnectionManager,
}

impl AsyncRedisCrateClient {
    pub async fn new(host: &str) -> Self {
        let client = redis::Client::open(host).unwrap();
        let connection = ConnectionManager::new(client).await.unwrap();
        Self { connection }
    }

    pub async fn xadd(
        &mut self,
        key: &str,
        id: &str,
        items: &[(
            impl ToRedisArgs + std::marker::Send + std::marker::Sync,
            impl ToRedisArgs + std::marker::Send + std::marker::Sync,
        )],
    ) -> Result<Value, redis::RedisError> {
        self.connection.xadd(key, id, items).await
    }

    pub async fn xrange(
        &mut self,
        key: &str,
        start: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
        end: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
    ) -> Result<Value, redis::RedisError> {
        self.connection.xrange(key, start, end).await
    }
}
