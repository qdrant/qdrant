use std::fmt;
use std::time::Duration;

use reqwest::StatusCode;

/// Why inference failed. Callers map this into their own error type (`StorageError` in qdrant,
/// `tonic::Status` in qdrant-serverless), so it carries data rather than ready-made messages.
#[derive(Debug)]
pub enum InferenceError {
    /// Some inputs need the inference service, but no address is configured.
    NotConfigured,
    /// An input a local model can't embed (wrong data type, invalid options).
    InvalidInput(String),
    /// The request did not complete: it couldn't be sent, timed out, or its reply couldn't be read.
    Transport(TransportError),
    /// The inference service replied with a non-success status.
    Rejected(Rejection),
    /// The inference service replied with success, but the reply is unusable.
    InvalidResponse(String),
    /// A bug or a failure of the local runtime.
    Internal(String),
}

#[derive(Debug)]
pub struct TransportError {
    pub stage: TransportStage,
    /// Whether the request ran out of time (reqwest's timeout).
    pub timeout: bool,
    /// The underlying error, as text.
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportStage {
    /// Sending the request or receiving the reply's head.
    Send,
    /// Reading the reply's body.
    ReadBody,
}

/// A non-success reply of the inference service.
#[derive(Debug)]
pub struct Rejection {
    pub status: StatusCode,
    /// The reply body as text.
    pub body: String,
    /// The `error` field of a JSON error body, if the body is one.
    pub error: Option<String>,
    /// The `Retry-After` header: seconds or an HTTP date.
    pub retry_after: Option<Duration>,
}

impl Rejection {
    /// The error description: the `error` field of a JSON body, or else the raw body.
    pub fn message(&self) -> &str {
        self.error.as_deref().unwrap_or(&self.body)
    }
}

impl InferenceError {
    pub(crate) fn transport(stage: TransportStage, err: &reqwest::Error) -> Self {
        Self::Transport(TransportError {
            stage,
            timeout: err.is_timeout(),
            message: err.to_string(),
        })
    }
}

impl fmt::Display for InferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConfigured => f.write_str("inference service address is not configured"),
            Self::InvalidInput(message) => f.write_str(message),
            Self::Transport(TransportError {
                stage,
                timeout: _,
                message,
            }) => match stage {
                TransportStage::Send => write!(f, "failed to send inference request: {message}"),
                TransportStage::ReadBody => {
                    write!(f, "failed to read inference response body: {message}")
                }
            },
            Self::Rejected(rejection) => write!(
                f,
                "inference service replied {}: {}",
                rejection.status,
                rejection.message()
            ),
            Self::InvalidResponse(message) => f.write_str(message),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for InferenceError {}
