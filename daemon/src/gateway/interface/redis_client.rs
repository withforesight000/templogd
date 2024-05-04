use redis::Value;


pub trait RedisClient {
    async fn xadd(&mut self, key: &str, id: &str, items: &[(&str, &str)]) -> Result<Value, redis::RedisError>;
}
