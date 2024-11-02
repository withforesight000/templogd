use crate::model::ambient_condition::AmbientCondition;

#[derive(Debug)]
pub enum DatastoreOperation {
    FetchAmbientConditions {
        start: String,
        end: String,
        resp: tokio::sync::oneshot::Sender<
            Result<std::collections::HashMap<String, AmbientCondition>, redis::RedisError>,
        >,
    },
    SaveAmbientCondition {
        ambient_condition: AmbientCondition,
    },
}
