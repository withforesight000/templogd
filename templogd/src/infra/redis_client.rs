use common::infra::redis_client::AsyncRedisCrateClient;
use redis::{RedisError, Value};

use crate::{gateway::interface::data_store::DataStore, model};

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
