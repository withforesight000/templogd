use std::fmt::Debug;
use std::marker::{Send, Sync};

use async_trait::async_trait;
use redis::{aio::ConnectionManager, cmd, AsyncCommands, RedisError, ToRedisArgs, Value};
use scopeguard::defer;
use tracing::{debug, info, instrument};

use crate::gateway::interface::redis::Redis;

pub struct AsyncRedisCrateClient {
    connection: ConnectionManager,
}

impl AsyncRedisCrateClient {
    #[instrument(parent = None)]
    pub async fn new(host: &str) -> Self {
        info!("Started");
        defer! {info!("Ended")}

        let client = redis::Client::open(host).unwrap();
        let connection = ConnectionManager::new(client).await.unwrap();
        Self { connection }
    }
}

#[async_trait]
impl Redis for AsyncRedisCrateClient {
    async fn fcall<K: ToRedisArgs + Send + Sync + 'static, A: ToRedisArgs + Send + Sync + 'static>(
        &mut self,
        function_name: &str,
        keys: &[K],
        args: &[A],
    ) -> Result<Value, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let res = cmd("FCALL")
            .arg(function_name)
            .arg(keys.len())
            .arg(keys)
            .arg(args)
            .query_async(&mut self.connection)
            .await
            .map_err(|e| {
                debug!("Error calling function: {}", e);
                e
            })?;

        Ok(res)
    }

    async fn function_load(&mut self, replace: bool, code: &str) -> Result<String, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

        let mut cmd = cmd("FUNCTION");
        cmd.arg("LOAD");
        if replace {
            cmd.arg("REPLACE");
        }
        let res = cmd
            .arg(code)
            .query_async(&mut self.connection)
            .await
            .map_err(|e| {
                debug!("Error loading function: {}", e);
                e
            })?;

        Ok(res)
    }

    #[instrument(parent = None, skip(self, items))]
    async fn xadd<T: ToRedisArgs + Send + Sync, U: ToRedisArgs + Send + Sync>(
        &mut self,
        key: &str,
        id: &str,
        items: &[(T, U)],
    ) -> Result<Value, RedisError> {
        debug!("Started");
        defer! {debug!("Ended")}

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

impl Debug for AsyncRedisCrateClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRedisCrateClient").field("connection", &"ConnectionManager").finish()
    }
}
