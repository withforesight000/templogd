use crate::model;

pub trait DataSource {
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<model::ambient_codition::AmbientCondition, Box<dyn std::error::Error>>;
}
