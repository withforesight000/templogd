#[derive(Debug)]
pub enum RedisCommand {
    Xadd {
        key: String,
        id: String,
        items: Vec<(String, String)>,
    },
}
