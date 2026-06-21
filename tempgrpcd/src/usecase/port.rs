use std::collections::HashMap;

use async_trait::async_trait;
use common::model::ambient_condition::AmbientCondition;

use super::error::UsecaseError;

/// Loads ambient conditions for a time range without sampling.
#[async_trait]
pub trait GetAmbientConditions {
    /// Fetch ambient conditions between the given Unix timestamps in seconds.
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}

/// Loads ambient conditions for a time range and applies Redis-side sampling.
#[async_trait]
pub trait GetAmbientConditionsWithSampling {
    /// Fetch sampled ambient conditions between the given Unix timestamps in seconds.
    async fn run(
        &self,
        start_time_seconds: i64,
        end_time_seconds: i64,
        samples: u64,
    ) -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}
