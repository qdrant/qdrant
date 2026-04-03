use std::io;

pub fn io_error_context(err: io::Error, context: impl Into<String>) -> io::Error {
    io::Error::new(err.kind(), IoErrorContext::new(err, context))
}

#[derive(Debug, thiserror::Error)]
#[error("{context}: {error}")]
pub struct IoErrorContext {
    context: String,
    error: io::Error,
}

impl IoErrorContext {
    fn new(error: io::Error, context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
            error,
        }
    }
}
