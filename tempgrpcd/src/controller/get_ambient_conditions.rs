use common::model::channel::datastore_operation::DatastoreOperation;
use scopeguard::defer;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

use crate::{
    pb::tempgrpcd::{tempgrpcd_server::Tempgrpcd, TempgrpcdRequest, TempgrpcdResponse},
    usecase,
};

#[derive(Debug)]
pub struct GetAmbientConditions {
    tx: tokio::sync::mpsc::Sender<common::model::channel::datastore_operation::DatastoreOperation>,
}

impl GetAmbientConditions {
    #[instrument(parent = None)]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Self {
        info!("Started");
        defer!{info!("Ended")}

        Self { tx }
    }
}

#[tonic::async_trait]
impl Tempgrpcd for GetAmbientConditions {
    #[instrument(parent = None)]
    async fn get_ambient_conditions(
        &self,
        request: Request<TempgrpcdRequest>,
    ) -> Result<Response<TempgrpcdResponse>, Status> {
        debug!("Started");
        defer!{debug!("Ended")}

        usecase::get_ambient_conditions::get_ambient_conditions(request, &self.tx).await
    }
}
