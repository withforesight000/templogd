use reqwest::{Error as ReqwestError, StatusCode};
use thiserror::Error;

/// Errors returned by outbound HTTP requests.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The request could not be sent.
    #[error("HTTP request failed")]
    Request(#[source] ReqwestError),
    /// The request exceeded the configured timeout.
    #[error("HTTP request timed out")]
    Timeout(#[source] ReqwestError),
    /// The server returned a non-success status and an optional bounded body preview.
    #[error("HTTP response returned status {0}")]
    StatusCodeError(StatusCode, String),
    /// The non-success response body could not be read.
    #[error("HTTP error response body could not be read for status {status}")]
    ResponseBody {
        status: StatusCode,
        #[source]
        source: ReqwestError,
    },
    /// A successful response body could not be decoded as JSON.
    #[error("HTTP response body could not be decoded as JSON")]
    Body(#[source] ReqwestError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_status_code_error() {
        let err = ClientError::StatusCodeError(reqwest::StatusCode::BAD_REQUEST, "oops".into());
        let msg = format!("{}", err);
        assert!(msg.contains("400"));
        assert!(!msg.contains("oops"));
    }
}
