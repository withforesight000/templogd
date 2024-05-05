use tonic::transport::Server;
use tonic_reflection::server::Builder;

use crate::{
    pb::tempgrpcd::tempgrpcd_server::TempgrpcdServer,
    usecase::get_ambient_conditions::GetAmbientConditions,
};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let greeter = GetAmbientConditions::default();

    println!("GreeterServer listening on {}", addr);
    Server::builder()
        .add_service(TempgrpcdServer::new(greeter))
        .add_service(
            Builder::configure()
                .register_encoded_file_descriptor_set(tonic::include_file_descriptor_set!(
                    "tempgrpcd"
                ))
                .build()
                .unwrap(),
        )
        .serve(addr)
        .await?;

    Ok(())
}
