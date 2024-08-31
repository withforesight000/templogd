
use common::model::channel::datastore_operation::DatastoreOperation;
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::{
    pb::tempgrpcd::{tempgrpcd_server::Tempgrpcd, TempgrpcdRequest, TempgrpcdResponse},
    usecase,
};

#[derive(Debug)]
pub struct GetAmbientConditions {
    tx: tokio::sync::mpsc::Sender<common::model::channel::datastore_operation::DatastoreOperation>,
}

impl GetAmbientConditions {
    #[instrument]
    pub fn new(tx: tokio::sync::mpsc::Sender<DatastoreOperation>) -> Self {
        Self { tx }
    }
}

#[tonic::async_trait]
impl Tempgrpcd for GetAmbientConditions {
    #[instrument]
    async fn get_ambient_conditions(
        &self,
        request: Request<TempgrpcdRequest>,
    ) -> Result<Response<TempgrpcdResponse>, Status> {
        usecase::get_ambient_conditions::get_ambient_conditions(request, &self.tx).await
    }
}
