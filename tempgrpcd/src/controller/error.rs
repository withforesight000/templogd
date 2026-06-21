use tonic::Status;

use crate::usecase::error::UsecaseError;
use crate::validator::error::ValidationError;

impl From<UsecaseError> for Status {
    fn from(error: UsecaseError) -> Self {
        match error {
            UsecaseError::DependencyUnavailable(message) => Status::unavailable(message),
            UsecaseError::Storage(_) => Status::internal("data store operation failed"),
            UsecaseError::Internal(message) => Status::internal(message),
        }
    }
}

impl From<ValidationError> for Status {
    fn from(error: ValidationError) -> Self {
        Status::invalid_argument(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_validation_error_to_invalid_argument() {
        let status: Status = ValidationError::invalid("bad request").into();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "bad request");
    }

    #[test]
    fn maps_unavailable_dependency_to_unavailable() {
        let status: Status = UsecaseError::dependency_unavailable("channel closed").into();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "channel closed");
    }

    #[test]
    fn maps_storage_error_without_exposing_details() {
        use common::model::repository::datastore::DataStoreError;

        let error = UsecaseError::from(DataStoreError::Unavailable("redis connection refused".into()));
        let status: Status = error.into();

        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "data store operation failed");
    }
}
