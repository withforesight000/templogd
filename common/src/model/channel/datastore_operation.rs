use crate::model::ambient_condition::AmbientCondition;
use crate::model::repository::datastore::DataStoreError;

/// Work items that the Redis fetch loop receives over its MPSC channel.
#[derive(Debug)]
pub enum DatastoreOperation {
    /// Ask the datastore worker to fetch ambient conditions from Redis.
    FetchAmbientConditions {
        start: String,
        end: String,
        /// Span of the gRPC request that originated this datastore operation.
        span: tracing::Span,
        resp: tokio::sync::oneshot::Sender<Result<std::collections::HashMap<String, AmbientCondition>, DataStoreError>>,
    },
    /// Ask the datastore worker to fetch ambient conditions using Redis sampling.
    FetchAmbientConditionsWithSampling {
        start: String,
        end: String,
        samples: String,
        /// Span of the gRPC request that originated this datastore operation.
        span: tracing::Span,
        resp: tokio::sync::oneshot::Sender<Result<std::collections::HashMap<String, AmbientCondition>, DataStoreError>>,
    },
    /// Persist one ambient condition reading to Redis.
    SaveAmbientCondition { ambient_condition: AmbientCondition },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn fetch_variant_carries_payloads() {
        let (tx, rx) =
            oneshot::channel::<Result<std::collections::HashMap<String, AmbientCondition>, DataStoreError>>();
        let op = DatastoreOperation::FetchAmbientConditions {
            start: "s".to_string(),
            end: "e".to_string(),
            span: tracing::Span::current(),
            resp: tx,
        };
        if let DatastoreOperation::FetchAmbientConditions { start, end, resp, .. } = op {
            assert_eq!(start, "s");
            assert_eq!(end, "e");
            resp.send(Ok(std::collections::HashMap::new())).unwrap();
        } else {
            panic!("unexpected variant");
        }
        rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn save_variant_carries_condition() {
        let cond = crate::model::ambient_condition::new(1.0, 2.0, 3.0);
        let op = DatastoreOperation::SaveAmbientCondition {
            ambient_condition: cond,
        };
        match op {
            DatastoreOperation::SaveAmbientCondition { ambient_condition } => {
                assert!((ambient_condition.get_temperature() - 1.0).abs() < f64::EPSILON)
            }
            _ => panic!("unexpected variant"),
        }
    }
}
