use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("{0}")]
    Invalid(String),
}

impl ValidationError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
