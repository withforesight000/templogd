use crate::model::ambient_codition::AmbientCondition;

#[derive(Debug)]
pub enum DatastoreOperation {
    SaveAmbientCondition { ambient_condition: AmbientCondition },
}
