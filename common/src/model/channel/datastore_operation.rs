use crate::model::ambient_condition::AmbientCondition;

#[derive(Debug)]
pub enum DatastoreOperation {
    FetchAmbientConditions {
        start: i64,
        end: i64,
        resp: tokio::sync::oneshot::Sender<
            Result<std::collections::HashMap<String, AmbientCondition>, redis::RedisError>,
        >,
    },
    FetchAmbientConditionsWithSampling {
        start: i64,
        end: i64,
        samples: u32,
        resp: tokio::sync::oneshot::Sender<
            Result<std::collections::HashMap<String, AmbientCondition>, redis::RedisError>,
        >,
    },
    SaveAmbientCondition {
        ambient_condition: AmbientCondition,
    },
}
