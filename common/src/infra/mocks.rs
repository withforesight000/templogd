use std::error::Error;

use ::redis::{RedisError, ToRedisArgs};
use mockall::predicate::*;
use mockall::*;

use crate::gateway::interface::nature_remo::NatureRemo;
use crate::gateway::interface::redis::Redis;
use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

// Mock the AsyncRedisCrateClient
mock! {
    pub AsyncRedisCrateClient {}

    #[async_trait::async_trait]
    impl crate::gateway::interface::redis::Redis for AsyncRedisCrateClient {
        async fn xadd<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
            &mut self,
            key: &str,
            id: &str,
            items: &[(T, U)],
        ) -> Result<::redis::Value, RedisError>;

        async fn xrange<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
            &mut self,
            key: &str,
            start: T,
            end: U,
        ) -> Result<::redis::Value, RedisError>;
    }
}

// Mock the NullRedisClient
mock! {
    pub NullRedisClient {}

    #[async_trait::async_trait]
    impl Redis for NullRedisClient {
        async fn xadd<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
            &mut self,
            key: &str,
            id: &str,
            items: &[(T, U)],
        ) -> Result<::redis::Value, RedisError>;

        async fn xrange<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
            &mut self,
            key: &str,
            start: T,
            end: U,
        ) -> Result<::redis::Value, RedisError>;
    }
}

// Mock the NatureRemoClient
mock! {
    pub NatureRemoClient {}

    #[async_trait::async_trait]
    impl NatureRemo for NatureRemoClient {
        async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn Error>>;
    }
}
