use std::collections::HashMap;

use redis::{from_redis_value, ToRedisArgs};

use crate::model;

impl model::repository::ambient_condition::AmbientCondition
    for common::infra::redis_client::AsyncRedisCrateClient
{
    async fn fetch_ambient_conditions_between_start_and_end(
        &mut self,
        start: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
        end: impl ToRedisArgs + std::marker::Send + std::marker::Sync,
    ) -> Result<
        HashMap<String, common::model::ambient_condition::AmbientCondition>,
        redis::RedisError,
    > {
        let res = self.xrange("ambient_condition", start, end).await;
        match res {
            Ok(values) => {
                let mut ambient_conditions = HashMap::new();
                for value in values.as_sequence().unwrap() {
                    let seq = value.clone().into_sequence().unwrap();
                    let k = from_redis_value(&seq[0]).unwrap();
                    let v = seq[1].clone().into_sequence().unwrap();
                    ambient_conditions.insert(
                        k,
                        common::model::ambient_condition::new(
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
