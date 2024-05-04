use redis::{aio::ConnectionManager, AsyncCommands, RedisError, ToRedisArgs, Value};

use crate::{gateway::interface::data_store::DataStore, model};

pub struct AsyncRedisCrateClient {
    connection: ConnectionManager,
}

impl AsyncRedisCrateClient {
    pub async fn new(host: &str) -> Self {
        let client = redis::Client::open(host).unwrap();
        let connection = ConnectionManager::new(client).await.unwrap();
        Self { connection }
    }

    async fn xadd(
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
}

impl DataStore for AsyncRedisCrateClient {
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: model::ambient_codition::AmbientCondition,
    ) -> Result<Value, RedisError> {
        let key = "ambient_condition";
        let id = "*";
        let items = vec![
            ("temperature", ambient_condition.get_temperature()),
            ("humidity", ambient_condition.get_humidity()),
            ("illumination", ambient_condition.get_illumination()),
        ];
        self.xadd(key, id, items.as_slice()).await
    }
}
