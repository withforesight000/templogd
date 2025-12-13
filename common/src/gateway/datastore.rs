use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use redis::{RedisError, ToRedisArgs, from_redis_value};
use scopeguard::defer;
use tracing::{debug, error, info, instrument};

use super::interface::redis::Redis;
use crate::model::ambient_condition::{self, AmbientCondition as AmbientConditionModel};
use crate::model::repository::datastore::DataStoreRepository;

#[derive(Debug)]
pub struct DataStore<R: Redis + Send + Debug> {
    redis_client: R,
}

impl<R: Redis + Send + Debug> DataStore<R> {
    #[instrument(parent = None, skip(redis_client))]
    pub async fn new(redis_client: R) -> Self {
        Self { redis_client }
    }

    #[instrument(parent = None)]
    pub async fn load_function_xrange_with_sampling(&mut self, code: &str) -> Result<(), RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res = self.redis_client.function_load(true, code).await;
        match res {
            Ok(script) => {
                info!("Loaded Lua script for xrange_with_sampling: {}", script);
                Ok(())
            }
            Err(e) => {
                error!("Failed to load Lua script: {}", e);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl<R: Redis + Send + Debug> DataStoreRepository for DataStore<R> {
    #[instrument(parent = None)]
    async fn fetch_ambient_conditions<T: ToRedisArgs + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res: Result<redis::Value, RedisError> = self.redis_client.xrange("ambient_condition", start, end).await;
        match res {
            Ok(values) => Ok(parse_ambient_conditions(&values)),
            Err(e) => Err(e),
        }
    }

    #[instrument(parent = None)]
    async fn fetch_ambient_conditions_with_sampling<T: ToRedisArgs + Send + Sync + 'static + Debug>(
        &mut self,
        start: T,
        end: T,
        samples: T,
    ) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res: Result<redis::Value, RedisError> =
            self.redis_client.fcall("xrange_with_sampling", &["ambient_condition"], &[start, end, samples]).await;
        match res {
            Ok(values) => Ok(parse_ambient_conditions(&values)),
            Err(e) => Err(e),
        }
    }

    #[instrument(parent = None)]
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

        let res = self.redis_client.xadd(key, id, items.as_slice()).await;
        info!("Saved ambient condition to Redis: {:?}", res);
        res
    }
}

fn parse_ambient_conditions(values: &redis::Value) -> HashMap<String, AmbientConditionModel> {
    let mut ambient_conditions = HashMap::new();
    for value in values.as_sequence().unwrap() {
        let seq = value.clone().into_sequence().unwrap();
        let k: String = from_redis_value(&seq[0]).unwrap();
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
    ambient_conditions
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;
    use redis::{ErrorKind, Value};

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
        RedisError::from((ErrorKind::IoError, "io"))
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
}
