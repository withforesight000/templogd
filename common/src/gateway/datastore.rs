use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use redis::{from_redis_value, RedisError, ToRedisArgs};
use scopeguard::defer;
use tracing::{debug, error, info, instrument};

use super::interface::redis::Redis;
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::datastore::DataStoreRepository;

#[derive(Debug)]
pub struct DataStore<R: Redis + Send + Debug> {
    redis_client: R,
}

impl<R: Redis + Send + Debug> DataStore<R> {
    #[instrument(parent = None, skip(redis_client))]
    pub async fn new(redis_client: R) -> Self {
        Self { redis_client }
    }

    #[instrument(parent = None)]
    pub async fn load_function_xrange_with_sampling(&mut self, code: &str) -> Result<(), RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res = self.redis_client.function_load(true, code).await;
        match res {
            Ok(script) => {
                info!("Loaded Lua script for xrange_with_sampling: {}", script);
                Ok(())
            }
            Err(e) => {
                error!("Failed to load Lua script: {}", e);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl<R: Redis + Send + Debug> DataStoreRepository for DataStore<R> {
    #[instrument(parent = None)]
    async fn fetch_ambient_conditions<T: ToRedisArgs + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res: Result<redis::Value, RedisError> = self.redis_client.xrange("ambient_condition", start, end).await;
        match res {
            Ok(values) => {
                let mut ambient_conditions = HashMap::new();
                for value in values.as_sequence().unwrap() {
                    let seq = value.clone().into_sequence().unwrap();
                    let k = from_redis_value(&seq[0]).unwrap();
                    let v = seq[1].clone().into_sequence().unwrap();
                    ambient_conditions.insert(
                        k,
                        ambient_condition::new(
                            from_redis_value(&v[1]).unwrap(),
                            from_redis_value(&v[3]).unwrap(),
                            from_redis_value(&v[5]).unwrap(),
                        ),
                    );
                }
                Ok(ambient_conditions)
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(parent = None)]
    async fn fetch_ambient_conditions_with_sampling<T: ToRedisArgs + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
        samples: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res: Result<redis::Value, RedisError> =
            self.redis_client.fcall("xrange_with_sampling", &["ambient_condition"], &[start, end, samples]).await;
        match res {
            Ok(values) => {
                let mut ambient_conditions = HashMap::new();
                for value in values.as_sequence().unwrap() {
                    let seq = value.clone().into_sequence().unwrap();
                    let k = from_redis_value(&seq[0]).unwrap();
                    let v = seq[1].clone().into_sequence().unwrap();
                    ambient_conditions.insert(
                        k,
                        ambient_condition::new(
                            from_redis_value(&v[1]).unwrap(),
                            from_redis_value(&v[3]).unwrap(),
                            from_redis_value(&v[5]).unwrap(),
                        ),
                    );
                }
                Ok(ambient_conditions)
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(parent = None)]
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: AmbientConditionModel,
    ) -> Result<redis::Value, RedisError> {
        let key = "ambient_condition";
        let id = "*";
        let items = vec![
            ("temperature", ambient_condition.get_temperature()),
            ("humidity", ambient_condition.get_humidity()),
            ("illumination", ambient_condition.get_illumination()),
        ];

        let res = self.redis_client.xadd(key, id, items.as_slice()).await;
        info!("Saved ambient condition to Redis: {:?}", res);
        res
    }
}
