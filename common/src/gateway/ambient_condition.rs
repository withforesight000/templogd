use std::collections::HashMap;
use std::error::Error;
use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{from_redis_value, RedisError, ToRedisArgs};

use crate::gateway::interface::{nature_remo::NatureRemo, redis::Redis};
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::ambient_condition::AmbientCondition;

pub struct AmbientConditionRepository<N: NatureRemo, R: Redis> {
    nature_remo_client: N,
    redis_client: R,
}

impl<N: NatureRemo, R: Redis> AmbientConditionRepository<N, R> {
    pub fn new(nature_remo_client: N, redis_client: R) -> AmbientConditionRepository<N, R> {
        AmbientConditionRepository {
            nature_remo_client,
            redis_client,
        }
    }
}

#[async_trait]
impl<N: NatureRemo + Sync + Send, R: Redis + Sync + Send> AmbientCondition for AmbientConditionRepository<N, R> {
    async fn fetch_current_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error>> {
        self.nature_remo_client.fetch_ambient_condition().await
    }

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
        self.redis_client.xadd(key, id, items.as_slice()).await
    }

    async fn fetch_ambient_conditions_between_start_and_end<
        T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
        U: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
    >(
        &mut self,
        start: T,
        end: U,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
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
}
