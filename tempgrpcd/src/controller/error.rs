use tonic::Status;

use crate::usecase::error::UsecaseError;
use crate::validator::error::ValidationError;
use common::model::repository::datastore::DataStoreError;

impl From<UsecaseError> for Status {
    /// Maps internal use-case failures to safe gRPC status values.
    fn from(error: UsecaseError) -> Self {
        match error {
            UsecaseError::DependencyUnavailable(message) => Status::unavailable(message),
            UsecaseError::ResourceExhausted(message) => Status::resource_exhausted(message),
            UsecaseError::DeadlineExceeded => Status::deadline_exceeded("request deadline exceeded"),
            UsecaseError::Cancelled => Status::cancelled("request cancelled"),
            UsecaseError::Storage(DataStoreError::ResultLimitExceeded { .. }) => {
                Status::resource_exhausted("ambient condition result limit exceeded")
            }
            UsecaseError::Storage(DataStoreError::DeadlineExceeded) => {
                Status::deadline_exceeded("request deadline exceeded")
            }
            UsecaseError::Storage(DataStoreError::Cancelled) => Status::cancelled("request cancelled"),
            UsecaseError::Storage(DataStoreError::Unavailable(_)) => Status::unavailable("data store operation failed"),
            UsecaseError::Internal(message) => Status::internal(message),
        }
    }
}

impl From<ValidationError> for Status {
    /// Converts a validation failure into an `InvalidArgument` status.
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
    fn maps_internal_error_to_internal_status() {
        let status: Status = UsecaseError::Internal("unexpected failure".into()).into();

        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "unexpected failure");
    }

    #[test]
    fn maps_storage_error_without_exposing_details() {
        use common::model::repository::datastore::DataStoreError;

        let redis_error = redis::RedisError::from((redis::ErrorKind::Io, "redis connection refused"));
        let error = UsecaseError::from(DataStoreError::from(redis_error));
        let status: Status = error.into();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "data store operation failed");
    }

    #[test]
    fn maps_capacity_and_deadline_errors() {
        let status: Status = UsecaseError::resource_exhausted("queue full").into();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        let status: Status = UsecaseError::from(DataStoreError::ResultLimitExceeded { limit: 5000 }).into();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        let status: Status = UsecaseError::DeadlineExceeded.into();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);

        let status: Status = UsecaseError::from(DataStoreError::DeadlineExceeded).into();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);

        let status: Status = UsecaseError::Cancelled.into();
        assert_eq!(status.code(), tonic::Code::Cancelled);

        let status: Status = UsecaseError::from(DataStoreError::Cancelled).into();
        assert_eq!(status.code(), tonic::Code::Cancelled);
    }
}
