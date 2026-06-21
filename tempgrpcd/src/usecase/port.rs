use std::collections::HashMap;

use async_trait::async_trait;
use common::model::ambient_condition::AmbientCondition;

use super::error::UsecaseError;

#[async_trait]
pub trait GetAmbientConditions {
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}

#[async_trait]
pub trait GetAmbientConditionsWithSampling {
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
        samples: u64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}
