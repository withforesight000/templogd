use async_trait::async_trait;
use mockall::automock;

use crate::gateway::interface::nature_remo::NatureRemo;
use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

#[derive(Debug)]
pub struct NullNatureRemoClient {}

impl Default for NullNatureRemoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl NullNatureRemoClient {
    pub fn new() -> Self {
        Self {}
    }
}

#[automock]
#[async_trait]
impl NatureRemo for NullNatureRemoClient {
    async fn fetch_ambient_condition(&self) -> Result<AmbientConditionModel, Box<dyn std::error::Error>> {
        panic!("BUGS: This should not be called: Not implemented");
    }
}
