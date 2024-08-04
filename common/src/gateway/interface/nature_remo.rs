use crate::model::ambient_condition::AmbientCondition as AmbientConditionModel;

use serde_json;

pub trait NatureRemo {
    // get devices from Nature Remo API
    // async fn get_devices(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>>;

    // fetch a current ambient condition from Nature Remo API
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<AmbientConditionModel, Box<dyn std::error::Error>>;
}
