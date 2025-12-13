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
    FetchAmbientConditionsWithSampling {
        start: String,
        end: String,
        samples: String,
        resp: tokio::sync::oneshot::Sender<
            Result<std::collections::HashMap<String, AmbientCondition>, redis::RedisError>,
        >,
    },
    SaveAmbientCondition {
        ambient_condition: AmbientCondition,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::RedisError;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn fetch_variant_carries_payloads() {
        let (tx, rx) = oneshot::channel::<Result<std::collections::HashMap<String, AmbientCondition>, RedisError>>();
        let op = DatastoreOperation::FetchAmbientConditions {
            start: "s".to_string(),
            end: "e".to_string(),
            resp: tx,
        };
        if let DatastoreOperation::FetchAmbientConditions { start, end, resp } = op {
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
