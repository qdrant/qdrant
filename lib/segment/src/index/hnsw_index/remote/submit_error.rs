use std::fmt;

/// Why [`super::RemoteIndexer::submit`] did not hand the task over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    /// A later submission may succeed: the entry point is unreachable, its queue is full, it
    /// is shutting down. Retried per `on_failure`.
    Transient(String),
    /// The entry point will never accept this task: too large for it, no common graph format.
    /// Built locally right away, without retrying.
    Permanent(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmitError::Transient(reason) => write!(f, "{reason}"),
            SubmitError::Permanent(reason) => write!(f, "{reason} (permanent)"),
        }
    }
}
