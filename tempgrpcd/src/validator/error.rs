use thiserror::Error;

/// Validation failures for inbound gRPC requests.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// The request was syntactically valid but not acceptable.
    #[error("{0}")]
    Invalid(String),
}

impl ValidationError {
    /// Create a validation error with a human-readable message.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
