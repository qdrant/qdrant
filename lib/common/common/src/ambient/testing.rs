//! Testing utilities.

use super::{Scope, unmeasured, unmeasured_guard};
use crate::reason::reason;

pub fn test<R>(f: impl FnOnce() -> R) -> R {
    unmeasured(reason("Test code"), f)
}

pub fn test_guard() -> Scope {
    unmeasured_guard(reason("Test code"))
}

/// Similar to [`test_guard`], but for `#[tokio::test]`.
///
/// Use this only at the _top level_ of an async test body.
/// Anywhere else, wrap the future with [`super::AmbientFutureExt`] instead.
#[track_caller]
pub fn async_test_guard() -> AsyncTestScope {
    let scope = test_guard();
    assert!(
        scope.is_outermost(),
        "an async test scope must be the outermost scope on its thread",
    );
    assert!(
        tokio::task::try_id().is_none(),
        "an async test scope belongs to the test body, not to a spawned task",
    );
    AsyncTestScope { _scope: scope }
}

/// Same as [Scope], but clippy won't complain about holding it across `.await`.
/// See [`async_test_guard`] for usage.
#[must_use]
pub struct AsyncTestScope {
    _scope: Scope,
}
