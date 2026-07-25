use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use redis::{RedisError, ToRedisArgs};

use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

/// Errors returned by datastore-backed repository operations.
#[derive(Debug, thiserror::Error)]
pub enum DataStoreError {
    /// The datastore could not complete the requested operation.
    #[error("data store operation failed")]
    Unavailable(#[source] RedisError),
}

impl From<RedisError> for DataStoreError {
    fn from(error: RedisError) -> Self {
        Self::Unavailable(error)
    }
}

#[async_trait]
pub trait DataStoreRepository {
    /// Fetch ambient conditions between two Redis stream timestamps.
    ///
    /// The arguments must be cloneable so the Redis adapter can replay a
    /// transiently failed request without changing its meaning.
    async fn fetch_ambient_conditions<
        T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + Debug,
    >(
        &mut self,
        start: T,
        end: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError>;

    /// Fetch ambient conditions between two Redis stream timestamps with sampling.
    ///
    /// The arguments must be cloneable so the Redis adapter can replay a
    /// transiently failed request without changing its meaning.
    async fn fetch_ambient_conditions_with_sampling<
        T: ToRedisArgs + Clone + std::marker::Send + std::marker::Sync + 'static + Debug,
    >(
        &mut self,
        start: T,
        end: T,
        samples: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError>;

    /// Save one ambient condition to Redis.
    ///
    /// TODO: The return type should be narrowed once the repository contract is cleaned up.
    async fn save_ambient_condition(
        &mut self,
        ambient_condition: AmbientConditionModel,
    ) -> Result<redis::Value, RedisError>;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use redis::ErrorKind;

    use super::DataStoreError;

    #[test]
    fn preserves_redis_error_as_source_without_exposing_details_in_display() {
        let error = DataStoreError::from(redis::RedisError::from((ErrorKind::Io, "connection refused")));

        assert_eq!(error.to_string(), "data store operation failed");
        assert_eq!(
            error.source().and_then(|source| source.downcast_ref::<redis::RedisError>()).unwrap().kind(),
            ErrorKind::Io
        );
    }
}
