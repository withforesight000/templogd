use std::collections::HashMap;

use common::{gateway::interface::redis::Redis, model::ambient_condition};
use redis::{from_redis_value, RedisError, ToRedisArgs};
use scopeguard::defer;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use common::model::ambient_condition::AmbientCondition as AmbientConditionModel;
use common::model::channel::datastore_operation::DatastoreOperation;

#[instrument(parent = None, skip(client))]
pub async fn run(
    mut client: impl Redis,
    mut rx: tokio::sync::mpsc::Receiver<DatastoreOperation>,
    cancellation_token: CancellationToken,
) {
    info!("Started");
    defer! {info!("Ended")}

    loop {
        tokio::select! {
            operation = rx.recv() => {
                debug!("Received operation from get_ambient_conditions task: {:?}", operation);
                if let Some(operation) = operation {
                    match operation {
                        DatastoreOperation::FetchAmbientConditions { start, end, resp } => {
                            let res = fetch_ambient_conditions_between_start_and_end(&mut client, start, end).await;
                            info!("Fetched ambient conditions from Redis: {:?}", res);

                            let result = resp.send(res);
                            if let Err(e) = result {
                                    error!("Failed to send ambient conditions to get_ambient_conditions task: {:?}", e);
                            }
                            info!("Sent ambient conditions to get_ambient_conditions task");
                        }
                        DatastoreOperation::SaveAmbientCondition { ambient_condition: _ } => {
                            panic!()
                        }
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                info!("confirmed cancellation token was cancelled");
                break;
            }
        }
    }
}

#[instrument(parent = None, skip(client, start, end))]
async fn fetch_ambient_conditions_between_start_and_end<
    T: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
    U: ToRedisArgs + std::marker::Send + std::marker::Sync + 'static,
>(
    client: &mut impl Redis,
    start: T,
    end: U,
) -> Result<HashMap<String, AmbientConditionModel>, RedisError> {
    debug!("Started");
    defer! {debug!("Ended")}

    let res: Result<redis::Value, RedisError> = client.xrange("ambient_condition", start, end).await;
    match res {
        Ok(values) => {
            let mut ambient_conditions = HashMap::new();
            for value in values.as_sequence().unwrap() {
                let seq = value.clone().into_sequence().unwrap();
                let k = from_redis_value(&seq[0]).unwrap();
                let v = seq[1].clone().into_sequence().unwrap();
                ambient_conditions.insert(
                    k,
                    ambient_condition::new(
                        from_redis_value(&v[1]).unwrap(),
                        from_redis_value(&v[3]).unwrap(),
                        from_redis_value(&v[5]).unwrap(),
                    ),
                );
            }
            Ok(ambient_conditions)
        }
        Err(e) => Err(e),
    }
}
