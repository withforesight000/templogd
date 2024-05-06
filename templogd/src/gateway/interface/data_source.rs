pub trait DataSource {
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error>>;
}
