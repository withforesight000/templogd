use std::fmt::Debug;
use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use async_trait::async_trait;
use redis::{ErrorKind, RedisError, ToRedisArgs, from_redis_value};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

use super::interface::redis::Redis;
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::datastore::DataStoreRepository;

const MAX_RETRY_ATTEMPTS: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Adapts Redis operations to the datastore repository used by the services.
#[derive(Debug)]
pub struct DataStore<R: Redis + Send + Debug> {
    redis_client: R,
}

impl<R: Redis + Send + Debug> DataStore<R> {
    /// Creates a datastore adapter around an already connected Redis client.
    #[instrument(level = "info", name = "gateway.redis.new", skip_all)]
    pub async fn new(redis_client: R) -> Self {
        Self { redis_client }
    }

    async fn retry_redis_operation<T, F>(
        &mut self,
        operation: &'static str,
        mut operation_fn: F,
    ) -> Result<T, RedisError>
    where
        F: for<'a> FnMut(&'a mut R) -> Pin<Box<dyn Future<Output = Result<T, RedisError>> + Send + 'a>>,
    {
        let mut attempt = 1;
        loop {
            match operation_fn(&mut self.redis_client).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < MAX_RETRY_ATTEMPTS && is_retryable_redis_error(&error) => {
                    warn!(
                        operation,
                        attempt,
                        max_attempts = MAX_RETRY_ATTEMPTS,
                        error = %error,
                        "Transient Redis error; retrying"
                    );
                    sleep(INITIAL_RETRY_DELAY * attempt).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Loads or replaces the Redis sampling function used by tempgrpcd.
    ///
    /// The Lua source is supplied by the caller so startup can render the
    /// configured function name before loading it into Redis.
    #[instrument(
        level = "info",
        name = "gateway.redis.load_function_xrange_with_sampling",
        skip_all,
        err
    )]
    pub async fn load_function_xrange_with_sampling(&mut self, code: &str) -> Result<(), RedisError> {
        let res = self.redis_client.function_load(true, code).await;
        match res {
            Ok(_) => {
                info!(
                    operation = "redis.function_load",
                    "Loaded Lua function xrange_with_sampling"
                );
                Ok(())
            }
            Err(e) => {
                error!(error = %e, operation = "redis.function_load", "Failed to load Lua function");
                Err(e)
            }
        }
    }
}

#[async_trait]
impl<R: Redis + Send + Debug> DataStoreRepository for DataStore<R> {
    #[instrument(level = "info", name = "gateway.redis.fetch_ambient_conditions", skip_all, err)]
    async fn fetch_ambient_conditions<T: ToRedisArgs + Clone + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        let result = self
            .retry_redis_operation("redis.fetch_ambient_conditions", |redis| {
                let start = start.clone();
                let end = end.clone();
                Box::pin(async move { redis.xrange("ambient_condition", start, end).await })
            })
            .await
            .map(|values| parse_ambient_conditions(&values));
        match &result {
            Ok(values) => info!(
                operation = "redis.fetch_ambient_conditions",
                count = values.len(),
                "Redis fetch parsed"
            ),
            Err(error) => error!(error = %error, operation = "redis.fetch_ambient_conditions", "Redis fetch failed"),
        }
        result
    }

    #[instrument(
        level = "info",
        name = "gateway.redis.fetch_ambient_conditions_with_sampling",
        skip_all,
        err
    )]
    async fn fetch_ambient_conditions_with_sampling<T: ToRedisArgs + Clone + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
        samples: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        let result = self
            .retry_redis_operation("redis.fetch_ambient_conditions_with_sampling", |redis| {
                let args = [start.clone(), end.clone(), samples.clone()];
                Box::pin(async move { redis.fcall("xrange_with_sampling", &["ambient_condition"], &args).await })
            })
            .await
            .map(|values| parse_ambient_conditions(&values));
        match &result {
            Ok(values) => info!(
                operation = "redis.fetch_ambient_conditions_with_sampling",
                count = values.len(),
                "Redis sampling result parsed"
            ),
            Err(error) => {
                error!(error = %error, operation = "redis.fetch_ambient_conditions_with_sampling", "Redis sampling fetch failed")
            }
        }
        result
    }

    /// Saves a reading with bounded retries for transient Redis failures.
    ///
    /// The stream entry uses the Redis-generated `*` ID, so retrying after an
    /// ambiguous I/O failure can create a duplicate entry. This is an
    /// intentional at-least-once delivery tradeoff.
    #[instrument(level = "info", name = "gateway.redis.save_ambient_condition", skip_all, err)]
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

        let res = self
            .retry_redis_operation("redis.save_ambient_condition", |redis| {
                let items = items.clone();
                Box::pin(async move { redis.xadd(key, id, items.as_slice()).await })
            })
            .await;
        match &res {
            Ok(_) => info!(
                operation = "redis.save_ambient_condition",
                "Ambient condition saved to Redis"
            ),
            Err(error) => error!(error = %error, operation = "redis.save_ambient_condition", "Redis save failed"),
        }
        res
    }
}

fn is_retryable_redis_error(error: &RedisError) -> bool {
    matches!(error.kind(), ErrorKind::Io)
}

/// Converts the Redis stream response into the domain model used by callers.
///
/// Redis stream entries are expected to contain an entry ID followed by the
/// temperature, humidity, and illumination field pairs.
#[instrument(level = "debug", name = "gateway.redis.parse_ambient_conditions", skip_all)]
fn parse_ambient_conditions(values: &redis::Value) -> HashMap<String, AmbientConditionModel> {
    let mut ambient_conditions = HashMap::new();
    for value in values.as_sequence().unwrap() {
        let seq = value.clone().into_sequence().unwrap();
        let k: String = from_redis_value(seq[0].clone()).unwrap();
        let v = seq[1].clone().into_sequence().unwrap();
        ambient_conditions.insert(
            k,
            ambient_condition::new(
                from_redis_value(v[1].clone()).unwrap(),
                from_redis_value(v[3].clone()).unwrap(),
                from_redis_value(v[5].clone()).unwrap(),
            ),
        );
    }
    ambient_conditions
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use redis::{ErrorKind, Value};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    mock! {
        #[derive(Debug)]
        pub RedisClient {}

        #[async_trait::async_trait]
        impl crate::gateway::interface::redis::Redis for RedisClient {
            async fn fcall<K: ToRedisArgs + Send + Sync + 'static, A: ToRedisArgs + Send + Sync + 'static>(
                &mut self,
                function: &str,
                keys: &[K],
                args: &[A],
            ) -> Result<Value, RedisError>;

            async fn function_load(&mut self, replace: bool, code: &str) -> Result<String, RedisError>;

            async fn xadd<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
                &mut self,
                key: &str,
                id: &str,
                items: &[(T, U)],
            ) -> Result<Value, RedisError>;

            async fn xrange<T: ToRedisArgs + Send + Sync + 'static, U: ToRedisArgs + Send + Sync + 'static>(
                &mut self,
                key: &str,
                start: T,
                end: U,
            ) -> Result<Value, RedisError>;
        }
    }

    fn sample_stream_value() -> Value {
        Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"1-0".to_vec()),
            Value::Array(vec![
                Value::BulkString(b"temperature".to_vec()),
                Value::BulkString(b"23.5".to_vec()),
                Value::BulkString(b"humidity".to_vec()),
                Value::BulkString(b"55.0".to_vec()),
                Value::BulkString(b"illumination".to_vec()),
                Value::BulkString(b"101.0".to_vec()),
            ]),
        ])])
    }

    fn redis_err() -> RedisError {
        RedisError::from((ErrorKind::Io, "io"))
    }

    #[tokio::test]
    async fn load_function_succeeds() {
        let mut redis = MockRedisClient::new();
        redis.expect_function_load().returning(|replace, code| {
            assert!(replace);
            assert_eq!(code, "lua-code");
            Ok("ok".to_string())
        });

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.load_function_xrange_with_sampling("lua-code").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn load_function_propagates_error() {
        let mut redis = MockRedisClient::new();
        redis.expect_function_load().returning(|_, _| Err(redis_err()));

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.load_function_xrange_with_sampling("lua-code").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_ambient_conditions_parses_stream() {
        let mut redis = MockRedisClient::new();
        redis.expect_xrange().returning(|_, _start: String, _end: String| Ok(sample_stream_value()));

        let mut datastore = DataStore::new(redis).await;
        let map = datastore.fetch_ambient_conditions("0-0".to_string(), "1-0".to_string()).await.unwrap();

        let condition = map.get("1-0").unwrap();
        assert!((condition.get_temperature() - 23.5).abs() < f64::EPSILON);
        assert!((condition.get_humidity() - 55.0).abs() < f64::EPSILON);
        assert!((condition.get_illumination() - 101.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn fetch_ambient_conditions_propagates_error() {
        let mut redis = MockRedisClient::new();
        redis.expect_xrange().returning(|_, _start: String, _end: String| Err(redis_err()));

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.fetch_ambient_conditions("0".to_string(), "1".to_string()).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retries_transient_fetch_error_before_succeeding() {
        let calls = Arc::new(AtomicUsize::new(0));
        let expected_calls = calls.clone();
        let mut redis = MockRedisClient::new();
        redis.expect_xrange().returning(move |_, _start: String, _end: String| {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(redis_err())
            } else {
                Ok(sample_stream_value())
            }
        });

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.fetch_ambient_conditions("0".to_string(), "1".to_string()).await;

        assert!(result.is_ok());
        assert_eq!(expected_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_transient_fetch_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let expected_calls = calls.clone();
        let mut redis = MockRedisClient::new();
        redis.expect_xrange().returning(move |_, _start: String, _end: String| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(RedisError::from((ErrorKind::InvalidClientConfig, "invalid request")))
        });

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.fetch_ambient_conditions("0".to_string(), "1".to_string()).await;

        assert!(result.is_err());
        assert_eq!(expected_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_with_sampling_calls_fcall() {
        let mut redis = MockRedisClient::new();
        redis.expect_fcall().returning(|function, keys: &[&str], args: &[String]| {
            assert_eq!(function, "xrange_with_sampling");
            assert_eq!(keys, &["ambient_condition"]);
            assert_eq!(args, &["0".to_string(), "1".to_string(), "10".to_string()]);
            Ok(sample_stream_value())
        });

        let mut datastore = DataStore::new(redis).await;
        let map = datastore
            .fetch_ambient_conditions_with_sampling("0".to_string(), "1".to_string(), "10".to_string())
            .await
            .unwrap();

        assert!(map.contains_key("1-0"));
    }

    #[test]
    fn parse_ambient_conditions_returns_map() {
        let val = sample_stream_value();
        let map = parse_ambient_conditions(&val);

        let condition = map.get("1-0").unwrap();
        assert!((condition.get_temperature() - 23.5).abs() < f64::EPSILON);
        assert!((condition.get_humidity() - 55.0).abs() < f64::EPSILON);
        assert!((condition.get_illumination() - 101.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn save_ambient_condition_passes_values_to_xadd() {
        let mut redis = MockRedisClient::new();
        redis.expect_xadd().returning(|key, id, items: &[(&str, f64)]| {
            assert_eq!(key, "ambient_condition");
            assert_eq!(id, "*");
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].0, "temperature");
            assert_eq!(items[1].0, "humidity");
            assert_eq!(items[2].0, "illumination");
            Ok(Value::Nil)
        });

        let mut datastore = DataStore::new(redis).await;
        let condition = ambient_condition::new(20.0, 40.0, 80.0);
        let result = datastore.save_ambient_condition(condition).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn retries_transient_save_error_before_succeeding() {
        let calls = Arc::new(AtomicUsize::new(0));
        let expected_calls = calls.clone();
        let mut redis = MockRedisClient::new();
        redis.expect_xadd().returning(move |_, _, _items: &[(&str, f64)]| {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 { Err(redis_err()) } else { Ok(Value::Nil) }
        });

        let mut datastore = DataStore::new(redis).await;
        let result = datastore.save_ambient_condition(ambient_condition::new(20.0, 40.0, 80.0)).await;

        assert!(result.is_ok());
        assert_eq!(expected_calls.load(Ordering::SeqCst), 3);
    }
}
