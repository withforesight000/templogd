use std::error::Error;

use redis::Value;

use crate::model;

use super::interface::{data_source::DataSource, data_store::DataStore};

#[derive(Clone, Debug)]
pub enum AmbientConditionRepository<S: DataSource, D: DataStore> {
    // data_source: S,
    // data_store: D,
    DataSource(S),
    DataStore(D),
}

impl<S: DataSource, D: DataStore> model::repository::ambient_condition::AmbientCondition
    for AmbientConditionRepository<S, D>
{
    async fn fetch_ambient_condition(
        &self,
    ) -> Result<common::model::ambient_condition::AmbientCondition, Box<dyn std::error::Error>> {
        match self {
            AmbientConditionRepository::DataSource(data_source) => {
                data_source.fetch_ambient_condition().await
            }
            AmbientConditionRepository::DataStore(_) => {
                panic!();
            }
        }
    }

    async fn save_ambient_condition(
        &mut self,
        ambient_condition: common::model::ambient_condition::AmbientCondition,
    ) -> Result<Value, impl Error> {
        match self {
            AmbientConditionRepository::DataSource(_) => {
                panic!();
            }
            AmbientConditionRepository::DataStore(data_store) => {
                data_store.save_ambient_condition(ambient_condition).await
            }
        }
    }
}
