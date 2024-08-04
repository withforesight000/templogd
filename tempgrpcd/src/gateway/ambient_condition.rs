use std::collections::HashMap;
use std::marker::{Send, Sync};

use common::gateway::interface::redis::Redis;
use common::model::ambient_condition;
use common::model::ambient_condition::AmbientCondition as AmbientConditionModel;
use model::repository::ambient_condition::AmbientCondition;
use crate::model;

use redis::{from_redis_value, RedisError, ToRedisArgs};

pub struct AmbientConditionRepository<R: Redis> {
    client: R,
}

impl <R: Redis> AmbientConditionRepository<R> {
    pub fn new(client: R) -> AmbientConditionRepository<R> {
        AmbientConditionRepository {
            client: client
        }
    }
}

impl<R: Redis> AmbientCondition
    for AmbientConditionRepository<R>
{
    async fn fetch_ambient_conditions_between_start_and_end(
        &mut self,
        start: impl ToRedisArgs + Send + Sync,
        end: impl ToRedisArgs + Send + Sync,
    ) -> Result<
        HashMap<String, AmbientConditionModel>,
        RedisError,
    > {
        let res: Result<redis::Value, RedisError> = self.client.xrange("ambient_condition", start, end).await;
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
