use thiserror::Error;

/// Why a quota operation did not go through.
///
/// Deliberately narrow: `storage` maps these onto its own error type, and the
/// mapping has to keep a limit rejection a client error (the client can free the
/// resource and retry) while a quota file it cannot read stays a server one.
#[derive(Debug, Error)]
pub enum QuotaError {
    /// An update was refused because a resource has reached its limit.
    #[error("{0}")]
    LimitReached(String),

    /// The config does not describe a quota that can be enforced.
    #[error("{0}")]
    InvalidConfig(String),

    /// The quota file could not be read or written.
    #[error("{0}")]
    Io(String),
}

pub type QuotaResult<T> = Result<T, QuotaError>;
