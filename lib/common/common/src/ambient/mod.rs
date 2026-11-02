//! Per-request ambient state, reached through a thread-local slot.

mod context;
mod future;
mod handoff;
pub mod hw;
#[cfg(debug_assertions)]
mod missing_scope;
pub mod net;
mod slot;
#[cfg(any(test, feature = "testing"))]
mod testing;
#[cfg(test)]
mod tests;
pub mod trace;

pub use context::AmbientContext;
pub use future::{AmbientFuture, AmbientFutureExt};
pub use handoff::{Handoff, current, parallel, unmeasured, unmeasured_guard};
pub use slot::Scope;
#[cfg(any(test, feature = "testing"))]
pub use testing::{AsyncTestScope, async_test_guard, test, test_guard};
