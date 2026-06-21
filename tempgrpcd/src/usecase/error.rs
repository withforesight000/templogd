use common::model::repository::datastore::DataStoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UsecaseError {
    #[error("dependency unavailable: {0}")]
    DependencyUnavailable(String),

    #[error(transparent)]
    Storage(#[from] DataStoreError),

    #[error("internal error: {0}")]
    Internal(String),
}

impl UsecaseError {
    pub fn dependency_unavailable(message: impl Into<String>) -> Self {
        Self::DependencyUnavailable(message.into())
    }
}
