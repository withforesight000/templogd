use common::model::repository::datastore::DataStoreError;
use thiserror::Error;

/// Errors that can occur while executing a tempgrpcd use case.
#[derive(Debug, Error)]
pub enum UsecaseError {
    /// The controller channel or response channel was closed before the work completed.
    #[error("dependency unavailable: {0}")]
    DependencyUnavailable(String),

    /// The service cannot admit more work without exceeding a configured bound.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// The request deadline elapsed.
    #[error("request deadline exceeded")]
    DeadlineExceeded,

    /// The caller cancelled the request.
    #[error("request cancelled")]
    Cancelled,

    /// The underlying datastore returned an error.
    #[error(transparent)]
    Storage(#[from] DataStoreError),

    /// An unexpected internal failure occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

impl UsecaseError {
    /// Create an error that indicates a required dependency disappeared mid-request.
    pub fn dependency_unavailable(message: impl Into<String>) -> Self {
        Self::DependencyUnavailable(message.into())
    }

    /// Creates an error explaining why the service could not admit more bounded work.
    ///
    /// The message should identify the exhausted queue, concurrency limit, or other
    /// capacity boundary without including request payloads or credentials.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted(message.into())
    }
}
