use std::fmt::Debug;
use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{AsyncCommands, RedisError, ToRedisArgs, Value, aio::ConnectionManager, cmd};
use tracing::{debug, instrument};

use crate::gateway::interface::redis::Redis;

#[async_trait]
pub trait RedisConnection: Debug + Send {
    async fn query_value(&mut self, cmd: redis::Cmd) -> Result<Value, RedisError>;
    async fn query_string(&mut self, cmd: redis::Cmd) -> Result<String, RedisError>;

    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<Value, RedisError>;

    async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        start: T,
        end: U,
    ) -> Result<Value, RedisError>;
}

pub struct RealRedisConnection {
    connection: ConnectionManager,
}

impl std::fmt::Debug for RealRedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealRedisConnection").field("connection", &"ConnectionManager").finish()
    }
}

#[async_trait]
impl RedisConnection for RealRedisConnection {
    async fn query_value(&mut self, cmd: redis::Cmd) -> Result<Value, RedisError> {
        cmd.query_async(&mut self.connection).await
    }

    async fn query_string(&mut self, cmd: redis::Cmd) -> Result<String, RedisError> {
        cmd.query_async(&mut self.connection).await
    }

    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<Value, RedisError> {
        self.connection.xadd(key, id, items).await
    }

    async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        start: T,
        end: U,
    ) -> Result<Value, RedisError> {
        self.connection.xrange(key, start, end).await
    }
}

/// Implements the shared Redis port using an asynchronous Redis connection.
pub struct AsyncRedisCrateClient<C: RedisConnection = RealRedisConnection> {
    connection: C,
}

impl AsyncRedisCrateClient<RealRedisConnection> {
    /// Opens a Redis connection manager for the supplied Redis URL.
    ///
    /// Startup configuration errors currently abort initialization because the
    /// daemon cannot perform useful work without Redis.
    pub async fn new(host: &str) -> Self {
        let client = redis::Client::open(host).unwrap();
        let connection = ConnectionManager::new(client).await.unwrap();
        Self {
            connection: RealRedisConnection { connection },
        }
    }
}

impl<C: RedisConnection> AsyncRedisCrateClient<C> {
    /// Builds a Redis client around a connection, primarily for in-process tests.
    pub fn new_with_connection(connection: C) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl<C: RedisConnection> Redis for AsyncRedisCrateClient<C> {
    #[instrument(name = "redis.fcall", skip_all, err)]
    async fn fcall<K: ToRedisArgs + Send + Sync + 'static, A: ToRedisArgs + Send + Sync + 'static>(
        &mut self,
        function_name: &str,
        keys: &[K],
        args: &[A],
    ) -> Result<Value, RedisError> {
        let mut c = cmd("FCALL");
        c.arg(function_name).arg(keys.len()).arg(keys).arg(args);

        self.connection.query_value(c).await.map_err(|e| {
            debug!("Error calling function: {}", e);
            e
        })
    }

    #[instrument(name = "redis.function_load", skip_all, err)]
    async fn function_load(&mut self, replace: bool, code: &str) -> Result<String, RedisError> {
        let mut c = cmd("FUNCTION");
        c.arg("LOAD").arg(if replace { "REPLACE" } else { "" }).arg(code);

        self.connection.query_string(c).await.map_err(|e| {
            debug!("Error loading function: {}", e);
            e
        })
    }

    #[instrument(name = "redis.xadd", skip_all, err)]
    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<Value, RedisError> {
        self.connection.xadd(key, id, items).await
    }

    async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        start: T,
        end: U,
    ) -> Result<Value, RedisError> {
        self.connection.xrange(key, start, end).await
    }
}

impl<C: RedisConnection> Debug for AsyncRedisCrateClient<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRedisCrateClient").field("connection", &"RedisConnection").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Value;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default, Clone)]
    struct MockConnection {
        last_query: Arc<Mutex<Option<Vec<u8>>>>,
        last_xadd: Arc<Mutex<Option<(String, String)>>>,
        last_xrange: Arc<Mutex<Option<(String, String, String)>>>,
    }

    impl MockConnection {
        fn last_query_as_string(&self) -> Option<String> {
            self.last_query.lock().unwrap().as_ref().map(|b| String::from_utf8_lossy(b).to_string())
        }
    }

    #[async_trait]
    impl RedisConnection for MockConnection {
        async fn query_value(&mut self, cmd: redis::Cmd) -> Result<Value, RedisError> {
            *self.last_query.lock().unwrap() = Some(cmd.get_packed_command());
            Ok(Value::Nil)
        }

        async fn query_string(&mut self, cmd: redis::Cmd) -> Result<String, RedisError> {
            *self.last_query.lock().unwrap() = Some(cmd.get_packed_command());
            Ok("OK".to_string())
        }

        async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
            &mut self,
            key: &str,
            id: &str,
            _items: &[(T, U)],
        ) -> Result<Value, RedisError> {
            *self.last_xadd.lock().unwrap() = Some((key.to_string(), id.to_string()));
            Ok(Value::Nil)
        }

        async fn xrange<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
            &mut self,
            key: &str,
            start: T,
            end: U,
        ) -> Result<Value, RedisError> {
            let mut start_buf: Vec<Vec<u8>> = Vec::new();
            start.write_redis_args(&mut start_buf);
            let mut end_buf: Vec<Vec<u8>> = Vec::new();
            end.write_redis_args(&mut end_buf);

            let start_bytes: Vec<u8> = start_buf.iter().flat_map(|v| v.clone()).collect();
            let end_bytes: Vec<u8> = end_buf.iter().flat_map(|v| v.clone()).collect();

            *self.last_xrange.lock().unwrap() = Some((
                key.to_string(),
                String::from_utf8_lossy(&start_bytes).to_string(),
                String::from_utf8_lossy(&end_bytes).to_string(),
            ));
            Ok(Value::Nil)
        }
    }

    // Creating a client with an invalid URL should panic because of unwraps.
    #[tokio::test]
    #[should_panic]
    async fn new_panics_on_invalid_url() {
        let _ = AsyncRedisCrateClient::new("not-a-redis-url").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fcall_builds_command_and_delegates() {
        let mock = MockConnection::default();
        let spy = mock.clone();
        let mut client = AsyncRedisCrateClient::new_with_connection(mock);

        let _ = client.fcall("my_func", &["key1".to_string()], &["arg1".to_string()]).await.unwrap();

        let packed = spy.last_query_as_string().unwrap();
        assert!(packed.contains("FCALL"));
        assert!(packed.contains("my_func"));
        assert!(packed.contains("key1"));
        assert!(packed.contains("arg1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn function_load_replace_and_non_replace_paths() {
        let mock = MockConnection::default();
        let spy = mock.clone();
        let mut client = AsyncRedisCrateClient::new_with_connection(mock);

        let _ = client.function_load(true, "lua-code").await.unwrap();
        let packed = spy.last_query_as_string().unwrap();
        assert!(packed.contains("FUNCTION"));
        assert!(packed.contains("LOAD"));
        assert!(packed.contains("REPLACE"));
        assert!(packed.contains("lua-code"));

        let mock = MockConnection::default();
        let spy = mock.clone();
        let mut client = AsyncRedisCrateClient::new_with_connection(mock);
        let _ = client.function_load(false, "lua-code").await.unwrap();
        let packed = spy.last_query_as_string().unwrap();
        assert!(packed.contains("FUNCTION"));
        assert!(packed.contains("LOAD"));
        assert!(packed.contains("lua-code"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn xadd_and_xrange_delegate_to_connection() {
        let mock = MockConnection::default();
        let spy = mock.clone();
        let mut client = AsyncRedisCrateClient::new_with_connection(mock);

        let _ = client.xadd("ambient_condition", "*", &[("temperature", "1.0")]).await.unwrap();
        assert_eq!(
            *spy.last_xadd.lock().unwrap(),
            Some(("ambient_condition".to_string(), "*".to_string()))
        );

        let _ = client.xrange("ambient_condition", "0-0".to_string(), "1-0".to_string()).await.unwrap();
        let (key, start, end) = spy.last_xrange.lock().unwrap().clone().unwrap();
        assert_eq!(key, "ambient_condition");
        assert!(start.contains("0-0"));
        assert!(end.contains("1-0"));
    }
}
