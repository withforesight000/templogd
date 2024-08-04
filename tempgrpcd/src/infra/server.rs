use common::model::ambient_condition::AmbientCondition;
use tonic::transport::Server;
use tonic_reflection::server::Builder;
use tracing::info;

use crate::{
    controller::{self, get_ambient_conditions},
    pb::tempgrpcd::tempgrpcd_server::TempgrpcdServer,
};

pub async fn run() {
    let (tx, rx) = tokio::sync::mpsc::channel(32);

    let addr = "0.0.0.0:50051".parse().unwrap();
    let ambient_condition_repository = get_ambient_conditions::GetAmbientConditions::new(tx);

    info!("GreeterServer listening on {}", addr);

    let _task_which_fetches_from_redis = tokio::spawn(async move {
        let address = format!("redis://{}:{}", "redis", 6379);
        let client = common::infra::redis_client::AsyncRedisCrateClient::new(&address).await;
        let ambient_repository_repo = crate::gateway::ambient_condition::AmbientConditionRepository::new(client);
        controller::fetch_from_redis::run(ambient_repository_repo, rx).await
    });

    let _task_grpc_server = Server::builder()
        .add_service(TempgrpcdServer::new(ambient_condition_repository))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(tonic::include_file_descriptor_set!(
                    "tempgrpcd"
                ))
                .build()
                .unwrap(),
        )
        .serve(addr)
        .await;
}
