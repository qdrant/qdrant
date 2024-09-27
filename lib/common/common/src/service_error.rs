use std::backtrace::Backtrace;

use thiserror::Error;

pub type ServiceResult<T> = Result<T, ServiceError>;

#[derive(Debug, Clone, Error, Eq, PartialEq)]
#[error("Service runtime error: {description}")]
pub struct ServiceError {
    pub description: String,
    pub backtrace: Option<String>,
}

impl ServiceError {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            backtrace: Some(Backtrace::force_capture().to_string()),
        }
    }
}

pub trait Context<T> {
    /// Converts [`Result::Err`] or [`None`] into an [`ServiceError`] with an
    /// additional context string. Keeps the original error message, but
    /// prepends it with the `context`.
    fn context(self, context: &str) -> Result<T, ServiceError>;

    /// Similar to [`Context::context`], but the context string is evaluated
    /// lazily.
    fn with_context(self, context: impl FnOnce() -> String) -> Result<T, ServiceError>;
}

impl<T, E: std::error::Error + 'static> Context<T> for Result<T, E> {
    fn context(self, context: &str) -> Result<T, ServiceError> {
        self.map_err(|e| wrap(e, context))
    }

    fn with_context(self, context: impl FnOnce() -> String) -> Result<T, ServiceError> {
        self.map_err(|e| wrap(e, &context()))
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, context: &str) -> Result<T, ServiceError> {
        self.ok_or_else(|| ServiceError::new(context))
    }

    fn with_context(self, context: impl FnOnce() -> String) -> Result<T, ServiceError> {
        self.ok_or_else(|| ServiceError::new(context()))
    }
}

/// Wrap the error, prepending the `context` to the error message.
/// Keep the original backtrace when possible.
fn wrap(mut e: impl std::error::Error + 'static, context: &str) -> ServiceError {
    if let Some(se) = e
        .source()
        .and_then(|source| source.downcast_ref::<ServiceError>())
    {
        return ServiceError {
            description: format!("{context}: {}", se.description.clone()),
            backtrace: se.backtrace.clone(),
        };
    }

    if let Some(se) = (&mut e as &mut dyn std::error::Error).downcast_mut::<ServiceError>() {
        return ServiceError {
            description: format!("{context}: {}", se.description),
            backtrace: std::mem::take(&mut se.backtrace),
        };
    }

    ServiceError {
        description: format!("{context}: {e}"),
        backtrace: Some(Backtrace::force_capture().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[allow(clippy::enum_variant_names)]
    enum TestError {
        #[error("IoError: {0}")]
        IoError(#[source] std::io::Error),
        #[error("{0}")]
        ServiceError(#[from] ServiceError),
        #[error("EmptyError")]
        EmptyError,
    }

    fn err_empty() -> Result<(), TestError> {
        Err(TestError::EmptyError)
    }

    fn err_io() -> Result<(), TestError> {
        Err(TestError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            "io error",
        )))
    }

    fn err_context_service(
        context: &str,
        e: Result<(), impl std::error::Error + 'static>,
    ) -> Result<(), ServiceError> {
        e.context(context)
    }

    fn err_context_aggregate(
        context: &str,
        e: Result<(), impl std::error::Error + 'static>,
    ) -> Result<(), TestError> {
        Ok(e.context(context)?)
    }

    fn check(res: Result<(), impl std::error::Error + 'static>, expected: &str) {
        assert_eq!(res.unwrap_err().to_string(), expected);
    }

    #[test]
    fn test_empty() {
        check(
            err_context_service("ctx1", err_empty()),
            "Service runtime error: ctx1: EmptyError",
        );

        check(
            err_context_aggregate("ctx1", err_empty()),
            "Service runtime error: ctx1: EmptyError",
        );

        check(
            err_context_service("ctx2", err_context_service("ctx1", err_empty())),
            "Service runtime error: ctx2: ctx1: EmptyError",
        );

        check(
            err_context_aggregate("ctx2", err_context_aggregate("ctx1", err_empty())),
            "Service runtime error: ctx2: ctx1: EmptyError",
        );

        check(
            err_context_aggregate("ctx2", err_context_service("ctx1", err_empty())),
            "Service runtime error: ctx2: ctx1: EmptyError",
        );

        check(
            err_context_service("ctx2", err_context_aggregate("ctx1", err_empty())),
            "Service runtime error: ctx2: ctx1: EmptyError",
        );
    }

    #[test]
    fn test_aggregate() {
        check(
            err_context_service("ctx1", err_io()),
            "Service runtime error: ctx1: IoError: io error",
        );

        check(
            err_context_aggregate("ctx1", err_io()),
            "Service runtime error: ctx1: IoError: io error",
        );

        check(
            err_context_service("ctx2", err_context_service("ctx1", err_io())),
            "Service runtime error: ctx2: ctx1: IoError: io error",
        );

        check(
            err_context_aggregate("ctx2", err_context_aggregate("ctx1", err_io())),
            "Service runtime error: ctx2: ctx1: IoError: io error",
        );

        check(
            err_context_aggregate("ctx2", err_context_service("ctx1", err_io())),
            "Service runtime error: ctx2: ctx1: IoError: io error",
        );

        check(
            err_context_service("ctx2", err_context_aggregate("ctx1", err_io())),
            "Service runtime error: ctx2: ctx1: IoError: io error",
        );
    }
}
