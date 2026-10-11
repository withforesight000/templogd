use std::collections::HashMap;

use async_trait::async_trait;
use common::model::ambient_condition::AmbientCondition;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::error::UsecaseError;

/// Request-scoped bounds propagated to the single Redis worker.
#[derive(Clone, Debug)]
pub struct QueryContext {
    pub start: String,
    pub end: String,
    pub deadline: Instant,
    pub cancellation: CancellationToken,
}

/// Loads ambient conditions for a time range without sampling.
// async-trait generates a must-use Future for each method; the Future already
// carries Rust's built-in must-use contract.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait GetAmbientConditions {
    /// Fetch ambient conditions between the given Unix timestamps in seconds.
    async fn run(&self, context: QueryContext) -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}

/// Loads ambient conditions for a time range and applies Redis-side sampling.
// async-trait generates a must-use Future for each method; the Future already
// carries Rust's built-in must-use contract.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait GetAmbientConditionsWithSampling {
    /// Fetch sampled ambient conditions between the given Unix timestamps in seconds.
    async fn run(&self, context: QueryContext, samples: u64)
    -> Result<HashMap<String, AmbientCondition>, UsecaseError>;
}
