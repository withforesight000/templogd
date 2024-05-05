use std::collections::HashMap;

use tonic::{Request, Response, Status};

use crate::pb::tempgrpcd::{
    tempgrpcd_server::Tempgrpcd, AmbientCondition, StatusCode, TempgrpcdRequest, TempgrpcdResponse,
};

#[derive(Default)]
pub struct GetAmbientConditions {}

#[tonic::async_trait]
impl Tempgrpcd for GetAmbientConditions {
    async fn get_ambient_conditions(
        &self,
        _request: Request<TempgrpcdRequest>,
    ) -> Result<Response<TempgrpcdResponse>, Status> {
        let mut conditions: HashMap<u64, AmbientCondition> = HashMap::new();
        conditions.insert(
            1714929064092,
            AmbientCondition {
                temperature: 25.0,
                humidity: 50.0,
                illumination: 123.0,
            },
        );

        Ok(Response::new(TempgrpcdResponse {
            version: 1,
            status: StatusCode::Ok as i32,
            ambient_conditions: conditions,
        }))
    }
}
