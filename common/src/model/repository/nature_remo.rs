use async_trait::async_trait;

use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

// async-trait generates a must-use Future for each method; the Future already
// carries Rust's built-in must-use contract.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait NatureRemo {
    // get devices from Nature Remo API
    // async fn get_devices(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>>;

    // fetch a current ambient condition from Nature Remo API
    async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn std::error::Error + Send>>;
}
