use std::error::Error;

use ::redis::{RedisError, ToRedisArgs};
use mockall::predicate::*;
use mockall::*;

use crate::gateway::interface::nature_remo::NatureRemo;
use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;
use crate::model::repository::ambient_condition::AmbientCondition;

// Mock the NatureRemoClient
mock! {
    pub NatureRemoClient {}

    #[async_trait::async_trait]
    impl NatureRemo for NatureRemoClient {
        async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error + Send>>;
    }
}

// Mock the RedisClient
mock! {
    pub AmbientCondition {}

    #[async_trait::async_trait]
    impl AmbientCondition for AmbientCondition {
        async fn fetch_current_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error>>;

        // fetch saved ambient conditions from redis
        async fn fetch_ambient_conditions_between_start_and_end<
            T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
            U: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
        >(
            &mut self,
            start: T,
            end: U,
        ) -> Result<std::collections::HashMap<String,AmbientConditionModel> , RedisError>;

        // save an ambient condition to redis
        // TODO: fix method signature
        async fn save_ambient_condition(
            &mut self,
            ambient_condition: AmbientConditionModel,
        ) -> Result<redis::Value, RedisError>;
    }
}
